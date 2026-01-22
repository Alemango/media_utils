#!/usr/bin/env python
# coding: utf-8

# ## Class Test
# 
# null

# # Test Class for EDA Refactor
# 

# ## Data Test

# In[2]:


!pip install seaborn

# Import necessary libraries
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from pandas.api.types import is_datetime64_any_dtype

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)

##RUN##

# ## Data Merger

# In[3]:


"""
Módulo para merge de DataFrames con Pandas.
"""

import pandas as pd
from typing import Dict, List, Union, Optional
import logging


class DataMerger:
    """Ejecuta merges entre DataFrames de Pandas."""

    def __init__(self, join_strategy: str = "left"):
        self.join_strategy = join_strategy
        self.logger = logging.getLogger(__name__)

    def merge(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        on: Union[str, List[str]],
        how: Optional[str] = None,
        drop_duplicate_columns: bool = True,
        suffixes: tuple = ('', '_right')
    ) -> pd.DataFrame:
        """Realiza merge entre dos DataFrames."""
        strategy = how if how is not None else self.join_strategy

        self.logger.info(f"Merge: {len(left_df)} x {len(right_df)} filas con '{strategy}'")

        # Identificar columnas duplicadas
        if drop_duplicate_columns:
            join_keys = [on] if isinstance(on, str) else on
            duplicate_cols = self.identify_duplicate_columns(left_df, right_df, exclude=join_keys)
            if duplicate_cols:
                self.logger.info(f"Eliminando {len(duplicate_cols)} columnas duplicadas")
                right_df = right_df.drop(columns=duplicate_cols)

        # Ejecutar merge
        merged_df = pd.merge(left_df, right_df, on=on, how=strategy, suffixes=suffixes)

        self.logger.info(f"Merge completado: {len(merged_df):,} filas")
        return merged_df

    def compare_join_strategies(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        on: str
    ) -> Dict:
        """Compara diferentes estrategias de join."""
        results = {}

        for strategy in ['inner', 'left', 'outer']:
            merged = pd.merge(left_df, right_df, on=on, how=strategy)
            null_pct = (merged.isnull().sum().sum() / (len(merged) * len(merged.columns)) * 100)

            results[strategy] = {
                'count': len(merged),
                'columns': len(merged.columns),
                'null_percentage': null_pct
            }

        # LEFT invertido
        merged_inv = pd.merge(right_df, left_df, on=on, how='left')
        null_pct_inv = (merged_inv.isnull().sum().sum() / (len(merged_inv) * len(merged_inv.columns)) * 100)

        results['left_inverted'] = {
            'count': len(merged_inv),
            'columns': len(merged_inv.columns),
            'null_percentage': null_pct_inv
        }

        self._print_comparison(results, len(left_df), len(right_df))
        return results

    def identify_duplicate_columns(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        exclude: Optional[List[str]] = None
    ) -> List[str]:
        """Identifica columnas duplicadas."""
        exclude = exclude or []
        left_cols = set(left_df.columns)
        right_cols = set(right_df.columns)
        duplicates = list((left_cols & right_cols) - set(exclude))
        return duplicates

    def validate_merge_quality(
        self,
        merged_df: pd.DataFrame,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        join_key: str
    ) -> Dict:
        """Valida calidad del merge."""
        return {
            'merged_count': len(merged_df),
            'left_count': len(left_df),
            'right_count': len(right_df),
            'match_rate': (len(merged_df) / len(left_df) * 100) if len(left_df) > 0 else 0,
            'null_join_keys': merged_df[join_key].isnull().sum(),
            'distinct_keys_merged': merged_df[join_key].nunique(),
            'distinct_keys_left': left_df[join_key].nunique(),
            'distinct_keys_right': right_df[join_key].nunique()
        }

    def _print_comparison(self, results: Dict, left_count: int, right_count: int):
        """Imprime comparación de estrategias."""
        print("\n" + "="*80)
        print("COMPARACIÓN DE ESTRATEGIAS DE JOIN")
        print("="*80)
        print(f"Left: {left_count:,} | Right: {right_count:,}")
        for strategy, metrics in results.items():
            print(f"\n{strategy.upper()}: {metrics['count']:,} filas, {metrics['null_percentage']:.2f}% nulos")

    def merge_multiple(
        self,
        dataframes: List[pd.DataFrame],
        on: Union[str, List[str]],
        how: Optional[str] = None,
        drop_duplicate_columns: bool = True,
        suffixes: Optional[List[str]] = None,
        validate_intermediate: bool = False
    ) -> pd.DataFrame:
        """
        Realiza merge secuencial de N DataFrames.

        Args:
            dataframes: Lista de DataFrames a unir (mínimo 2)
            on: Columna(s) clave para el join
            how: Estrategia de join ('left', 'inner', 'outer', 'right')
            drop_duplicate_columns: Eliminar columnas duplicadas automáticamente
            suffixes: Lista de sufijos para cada DataFrame (opcional)
                Si no se proporciona, se generan automáticamente (_df1, _df2, etc.)
            validate_intermediate: Si True, valida cada merge intermedio

        Returns:
            DataFrame resultante del merge de todos los DataFrames

        Raises:
            ValueError: Si se proporcionan menos de 2 DataFrames

        Example:
            merger = DataMerger(join_strategy='left')
            result = merger.merge_multiple(
                dataframes=[df_clients, df_cases, df_products, df_regions],
                on='uniqueId',
                how='left',
                validate_intermediate=True
            )
        """
        if len(dataframes) < 2:
            raise ValueError("Se requieren al menos 2 DataFrames para merge_multiple")

        strategy = how if how is not None else self.join_strategy

        self.logger.info(f"Iniciando merge múltiple de {len(dataframes)} DataFrames con estrategia '{strategy}'")

        # Generar sufijos si no se proporcionan
        if suffixes is None:
            suffixes = [f'_df{i}' for i in range(len(dataframes))]
        elif len(suffixes) != len(dataframes):
            self.logger.warning(
                f"Número de sufijos ({len(suffixes)}) no coincide con DataFrames ({len(dataframes)}). "
                f"Generando sufijos automáticamente."
            )
            suffixes = [f'_df{i}' for i in range(len(dataframes))]

        # Comenzar con el primer DataFrame
        result = dataframes[0].copy()
        self.logger.info(f"DataFrame inicial: {len(result):,} filas, {len(result.columns)} columnas")

        # Merge secuencial
        merge_stats = []

        for i, df_right in enumerate(dataframes[1:], start=1):
            rows_before = len(result)

            # Preparar sufijos para este merge
            current_suffixes = ('', suffixes[i])

            # Identificar y eliminar columnas duplicadas si es necesario
            if drop_duplicate_columns:
                join_keys = [on] if isinstance(on, str) else on
                duplicate_cols = self.identify_duplicate_columns(result, df_right, exclude=join_keys)
                if duplicate_cols:
                    self.logger.info(f"  Merge {i}: Eliminando {len(duplicate_cols)} columnas duplicadas")
                    df_right = df_right.drop(columns=duplicate_cols)

            # Ejecutar merge
            result = pd.merge(
                result,
                df_right,
                on=on,
                how=strategy,
                suffixes=current_suffixes
            )

            rows_after = len(result)

            # Estadísticas del merge
            stats = {
                'merge_step': i,
                'right_df_rows': len(dataframes[i]),
                'right_df_cols': len(dataframes[i].columns),
                'rows_before': rows_before,
                'rows_after': rows_after,
                'row_change': rows_after - rows_before,
                'row_change_pct': ((rows_after - rows_before) / rows_before * 100) if rows_before > 0 else 0
            }
            merge_stats.append(stats)

            self.logger.info(
                f"  Merge {i}/{len(dataframes)-1}: "
                f"{rows_before:,} + {len(dataframes[i]):,} -> {rows_after:,} filas"
            )

            # Validación intermedia opcional
            if validate_intermediate:
                validation = self._validate_merge_step(result, on, i)
                if validation['null_keys'] > 0:
                    self.logger.warning(
                        f"  Advertencia: {validation['null_keys']} claves nulas en paso {i}"
                    )

        self.logger.info(
            f"Merge múltiple completado: {len(result):,} filas, {len(result.columns)} columnas"
        )

        # Almacenar estadísticas para consulta posterior
        self._last_merge_stats = merge_stats

        return result

    def _validate_merge_step(
        self,
        df: pd.DataFrame,
        join_key: Union[str, List[str]],
        step: int
    ) -> Dict:
        """Valida un paso intermedio del merge."""
        key = join_key if isinstance(join_key, str) else join_key[0]
        return {
            'step': step,
            'row_count': len(df),
            'null_keys': df[key].isnull().sum() if key in df.columns else -1,
            'distinct_keys': df[key].nunique() if key in df.columns else -1
        }

    def get_merge_statistics(self) -> List[Dict]:
        """
        Retorna estadísticas del último merge múltiple ejecutado.

        Returns:
            Lista de diccionarios con estadísticas de cada paso del merge
        """
        return getattr(self, '_last_merge_stats', [])

    def compare_multiple_merge_strategies(
        self,
        dataframes: List[pd.DataFrame],
        on: Union[str, List[str]]
    ) -> Dict:
        """
        Compara diferentes estrategias de merge para N DataFrames.

        Args:
            dataframes: Lista de DataFrames a evaluar
            on: Columna(s) clave para el join

        Returns:
            Dict con resultados por estrategia

        Example:
            comparison = merger.compare_multiple_merge_strategies(
                [df1, df2, df3, df4],
                on='uniqueId'
            )
        """
        if len(dataframes) < 2:
            raise ValueError("Se requieren al menos 2 DataFrames")

        results = {}

        for strategy in ['inner', 'left', 'outer']:
            try:
                merged = self.merge_multiple(
                    dataframes=dataframes,
                    on=on,
                    how=strategy,
                    drop_duplicate_columns=True,
                    validate_intermediate=False
                )

                null_pct = (merged.isnull().sum().sum() / (len(merged) * len(merged.columns)) * 100) if len(merged) > 0 else 0

                results[strategy] = {
                    'count': len(merged),
                    'columns': len(merged.columns),
                    'null_percentage': round(null_pct, 2),
                    'merge_stats': self.get_merge_statistics()
                }
            except Exception as e:
                self.logger.error(f"Error en estrategia '{strategy}': {e}")
                results[strategy] = {'error': str(e)}

        self._print_multiple_comparison(results, dataframes)
        return results

    def _print_multiple_comparison(self, results: Dict, dataframes: List[pd.DataFrame]):
        """Imprime comparación de estrategias para merge múltiple."""
        print("\n" + "="*80)
        print("COMPARACIÓN DE ESTRATEGIAS - MERGE MÚLTIPLE")
        print("="*80)

        print(f"\nDataFrames de entrada:")
        for i, df in enumerate(dataframes):
            print(f"  DF{i}: {len(df):,} filas, {len(df.columns)} columnas")

        print("\nResultados por estrategia:")
        for strategy, metrics in results.items():
            if 'error' in metrics:
                print(f"\n{strategy.upper()}: ERROR - {metrics['error']}")
            else:
                print(f"\n{strategy.upper()}:")
                print(f"  Filas finales: {metrics['count']:,}")
                print(f"  Columnas: {metrics['columns']}")
                print(f"  % Nulos: {metrics['null_percentage']:.2f}%")

    def __repr__(self) -> str:
        return f"DataMerger(strategy='{self.join_strategy}')"

##RUN##

# ## Quality

# In[4]:


"""
Análisis de calidad de datos con Pandas.
"""

import pandas as pd
from typing import Dict
import logging


class DataQualityAnalyzer:
    """Analiza calidad de datos con Pandas."""

    def __init__(self, severity_thresholds: Dict = None):
        self.severity_thresholds = severity_thresholds or {
            'critical': 50, 'high': 20, 'moderate': 5, 'low': 0
        }
        self.logger = logging.getLogger(__name__)

    def analyze_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analiza valores nulos."""
        null_counts = df.isnull().sum()
        total = len(df)

        analysis = pd.DataFrame({
            'column': df.columns,
            'null_count': null_counts.values,
            'non_null_count': total - null_counts.values,
            'null_percentage': (null_counts.values / total * 100) if total > 0 else 0,
            'data_type': df.dtypes.values.astype(str)
        }).sort_values('null_percentage', ascending=False)

        return analysis

    def classify_null_severity(self, df: pd.DataFrame) -> Dict:
        """Clasifica nulos por severidad."""
        null_analysis = self.analyze_nulls(df)
        severity = {'critical': [], 'high': [], 'moderate': [], 'low': []}

        for _, row in null_analysis.iterrows():
            if row['null_count'] == 0:
                continue
            pct = row['null_percentage']
            col = row['column']

            if pct > self.severity_thresholds['critical']:
                severity['critical'].append(col)
            elif pct > self.severity_thresholds['high']:
                severity['high'].append(col)
            elif pct > self.severity_thresholds['moderate']:
                severity['moderate'].append(col)
            else:
                severity['low'].append(col)

        return severity

    def calculate_completeness_score(self, df: pd.DataFrame) -> float:
        """Calcula score de completitud."""
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        return ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0

    def suggest_null_strategies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sugiere estrategias de mitigación."""
        null_analysis = self.analyze_nulls(df)
        strategies = []

        for _, row in null_analysis[null_analysis['null_count'] > 0].iterrows():
            strategy, reason = self._determine_strategy(
                row['column'], row['null_percentage'], row['data_type']
            )
            strategies.append({
                'column': row['column'],
                'null_percentage': row['null_percentage'],
                'data_type': row['data_type'],
                'strategy': strategy,
                'reason': reason
            })

        return pd.DataFrame(strategies)

    def _determine_strategy(self, col: str, pct: float, dtype: str) -> tuple:
        """Determina estrategia según tipo."""
        if pct > 50:
            return "Drop column", f"{pct:.1f}% nulls - too high"

        if 'int' in dtype or 'float' in dtype:
            return "Impute (mean/median)", f"{pct:.1f}% nulls"
        elif 'object' in dtype or 'string' in dtype:
            return "Impute with Unknown/Mode", f"{pct:.1f}% nulls"
        elif 'bool' in dtype:
            return "Impute with False", f"{pct:.1f}% nulls"
        elif 'datetime' in dtype:
            return "Drop rows or default date", f"{pct:.1f}% nulls"

        return "Analyze manually", f"{pct:.1f}% nulls"

##RUN##

# ## Aggregator

# In[5]:


"""
Feature Engineering con Pandas.
"""

import pandas as pd
from typing import Dict, List
import logging


class FeatureAggregator:
    """Genera features agregadas con Pandas."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def aggregate_by_key(self, df: pd.DataFrame, group_by_col: str, aggregations: Dict) -> pd.DataFrame:
        """Agregación genérica."""
        return df.groupby(group_by_col).agg(aggregations).reset_index()

    def create_case_features(self, case_df: pd.DataFrame, group_by: str = "uniqueId") -> pd.DataFrame:
        """Crea 15+ features de casos."""
        agg_dict = {}

        # Conteos
        if 'caseId' in case_df.columns:
            agg_dict['caseId'] = ['count', 'nunique']

        # Métricas de rating
        if 'surveyRatingNumber' in case_df.columns:
            agg_dict['surveyRatingNumber'] = ['mean', 'min', 'max']

        # Aging
        if 'aging' in case_df.columns:
            agg_dict['aging'] = ['mean', 'max']

        # Agregación
        result = case_df.groupby(group_by).agg(agg_dict).reset_index()

        # Renombrar columnas
        result.columns = ['_'.join(col).strip('_') if col[1] else col[0] for col in result.columns.values]

        # Renombrar según convención
        rename_map = {
            'caseId_count': 'total_cases',
            'caseId_nunique': 'distinct_cases',
            'surveyRatingNumber_mean': 'avg_survey_rating',
            'surveyRatingNumber_min': 'min_survey_rating',
            'surveyRatingNumber_max': 'max_survey_rating',
            'aging_mean': 'avg_case_aging_days',
            'aging_max': 'max_case_aging_days'
        }

        result = result.rename(columns=rename_map)

        # Contadores condicionales
        if 'status' in case_df.columns:
            status_counts = case_df.groupby([group_by, 'status']).size().unstack(fill_value=0)
            if 'Open' in status_counts.columns:
                result = result.merge(status_counts[['Open']].rename(columns={'Open': 'open_cases_count'}),
                                    left_on=group_by, right_index=True, how='left')
            if 'Closed' in status_counts.columns:
                result = result.merge(status_counts[['Closed']].rename(columns={'Closed': 'closed_cases_count'}),
                                    left_on=group_by, right_index=True, how='left')

        # Priority
        if 'priority' in case_df.columns:
            priority_counts = case_df.groupby([group_by, 'priority']).size().unstack(fill_value=0)
            for priority_level in ['High', 'Medium', 'Low']:
                if priority_level in priority_counts.columns:
                    col_name = f'{priority_level.lower()}_priority_cases'
                    result = result.merge(priority_counts[[priority_level]].rename(columns={priority_level: col_name}),
                                        left_on=group_by, right_index=True, how='left')

        # Categorías distintas
        if 'category' in case_df.columns:
            cat_distinct = case_df.groupby(group_by)['category'].nunique().reset_index()
            cat_distinct.columns = [group_by, 'distinct_case_categories']
            result = result.merge(cat_distinct, on=group_by, how='left')

        # Fechas
        if 'caseDateCreated' in case_df.columns:
            date_agg = case_df.groupby(group_by)['caseDateCreated'].agg(['min', 'max']).reset_index()
            date_agg.columns = [group_by, 'first_case_date', 'most_recent_case_date']
            result = result.merge(date_agg, on=group_by, how='left')

        self.logger.info(f"Features creadas: {len(result.columns)-1} columnas")
        return result

    def create_count_features(
        self,
        df: pd.DataFrame,
        group_col: str,
        categorical_cols: List[str]
    ) -> pd.DataFrame:
        """
        Crea features de conteo para columnas categóricas.

        Args:
            df: DataFrame fuente
            group_col: Columna para agrupar
            categorical_cols: Lista de columnas categóricas a contar

        Returns:
            DataFrame con conteos por categoría

        Example:
            df_counts = aggregator.create_count_features(
                df, 'uniqueId', ['status', 'priority', 'category']
            )
        """
        if group_col not in df.columns:
            self.logger.warning(f"Columna de grupo '{group_col}' no existe")
            return pd.DataFrame()

        result = df[[group_col]].drop_duplicates()

        for col in categorical_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            # Crear crosstab
            counts = pd.crosstab(df[group_col], df[col])

            # Renombrar columnas
            counts.columns = [f'{col}_{str(val).replace(" ", "_").lower()}_count'
                            for val in counts.columns]

            # Merge con resultado
            result = result.merge(counts, left_on=group_col, right_index=True, how='left')

        self.logger.info(f"Features de conteo creadas para {len(categorical_cols)} columnas")

        return result

    def create_time_features(
        self,
        df: pd.DataFrame,
        group_col: str,
        date_cols: List[str]
    ) -> pd.DataFrame:
        """
        Crea features temporales (min, max, count) para columnas de fechas.

        Args:
            df: DataFrame fuente
            group_col: Columna para agrupar
            date_cols: Lista de columnas de fecha

        Returns:
            DataFrame con features temporales
        """
        if group_col not in df.columns:
            self.logger.warning(f"Columna de grupo '{group_col}' no existe")
            return pd.DataFrame()

        agg_dict = {}

        for col in date_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            agg_dict[col] = ['min', 'max', 'count']

        if not agg_dict:
            return df[[group_col]].drop_duplicates()

        result = df.groupby(group_col).agg(agg_dict).reset_index()

        # Aplanar columnas multinivel
        result.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                         for col in result.columns.values]

        # Renombrar según convención
        rename_map = {}
        for col in date_cols:
            rename_map[f'{col}_min'] = f'{col}_first'
            rename_map[f'{col}_max'] = f'{col}_last'

        result = result.rename(columns=rename_map)

        self.logger.info(f"Features temporales creadas para {len(date_cols)} columnas")

        return result

    def fill_missing_aggregations(self, df: pd.DataFrame, fill_values: Dict = None) -> pd.DataFrame:
        """Rellena valores faltantes."""
        if fill_values is None:
            fill_values = {
                'total_cases': 0, 'distinct_cases': 0,
                'open_cases_count': 0, 'closed_cases_count': 0,
                'high_priority_cases': 0, 'medium_priority_cases': 0, 'low_priority_cases': 0
            }

        return df.fillna({k: v for k, v in fill_values.items() if k in df.columns})

    def __repr__(self) -> str:
        """Representación string del agregador."""
        return "FeatureAggregator()"

##RUN##

# ## Correlations

# In[6]:


"""
Módulo para análisis de correlaciones con Pandas.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Tuple
import logging


class CorrelationAnalyzer:
    """
    Calcula correlaciones y detecta relaciones entre variables.

    Attributes:
        correlation_threshold: Umbral para correlaciones fuertes (default: 0.7)
    """

    def __init__(self, correlation_threshold: float = 0.7):
        self.correlation_threshold = correlation_threshold
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"CorrelationAnalyzer inicializado (threshold={correlation_threshold})")

    def calculate_correlation_matrix(
        self,
        df: pd.DataFrame,
        numeric_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Calcula matriz de correlación de Pearson.

        Args:
            df: DataFrame
            numeric_cols: Lista de columnas numéricas (None = auto-detectar)

        Returns:
            pandas DataFrame con matriz de correlación
        """
        if numeric_cols is None:
            numeric_cols = self._get_numeric_columns(df)

        if len(numeric_cols) < 2:
            self.logger.warning("Se necesitan al menos 2 columnas numéricas para correlación")
            return pd.DataFrame()

        self.logger.info(f"Calculando correlación para {len(numeric_cols)} columnas...")

        corr_matrix = df[numeric_cols].corr()

        self.logger.info("Matriz de correlación calculada")

        return corr_matrix

    def find_strong_correlations(
        self,
        corr_matrix: pd.DataFrame,
        threshold: Optional[float] = None
    ) -> List[Tuple[str, str, float]]:
        """
        Encuentra correlaciones fuertes en la matriz.

        Args:
            corr_matrix: Matriz de correlación
            threshold: Umbral (None = usar default)

        Returns:
            Lista de tuplas (var1, var2, correlation)
        """
        if threshold is None:
            threshold = self.correlation_threshold

        strong_corrs = []

        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = corr_matrix.iloc[i, j]

                if abs(corr_val) > threshold:
                    strong_corrs.append((
                        corr_matrix.columns[i],
                        corr_matrix.columns[j],
                        corr_val
                    ))

        self.logger.info(f"Encontradas {len(strong_corrs)} correlaciones fuertes (|r| > {threshold})")

        return strong_corrs

    def analyze_correlation_with_target(
        self,
        df: pd.DataFrame,
        target_col: str
    ) -> pd.DataFrame:
        """
        Analiza correlación de todas las variables con una variable objetivo.

        Args:
            df: DataFrame
            target_col: Columna objetivo

        Returns:
            pandas DataFrame con correlaciones ordenadas
        """
        if target_col not in df.columns:
            raise ValueError(f"La columna objetivo '{target_col}' no existe")

        numeric_cols = self._get_numeric_columns(df)

        if target_col not in numeric_cols:
            raise ValueError(f"La columna objetivo '{target_col}' no es numérica")

        corr_matrix = self.calculate_correlation_matrix(df, numeric_cols)

        if corr_matrix.empty:
            return pd.DataFrame()

        # Extraer correlaciones con target
        correlations = corr_matrix[target_col].drop(target_col)
        correlations_abs = correlations.abs().sort_values(ascending=False)

        result = pd.DataFrame({
            'variable': correlations_abs.index,
            'correlation': correlations[correlations_abs.index].values,
            'abs_correlation': correlations_abs.values
        })

        self.logger.info(f"Análisis de correlación con '{target_col}' completado")

        return result

    def detect_multicollinearity(
        self,
        df: pd.DataFrame,
        threshold: float = 0.9
    ) -> List[Tuple[str, str, float]]:
        """
        Detecta multicolinealidad (correlaciones muy altas entre predictores).

        Args:
            df: DataFrame
            threshold: Umbral de multicolinealidad (default: 0.9)

        Returns:
            Lista de pares con multicolinealidad
        """
        numeric_cols = self._get_numeric_columns(df)
        corr_matrix = self.calculate_correlation_matrix(df, numeric_cols)

        if corr_matrix.empty:
            return []

        multicollinear = []

        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_val = abs(corr_matrix.iloc[i, j])

                if corr_val > threshold:
                    multicollinear.append((
                        corr_matrix.columns[i],
                        corr_matrix.columns[j],
                        corr_val
                    ))

        self.logger.info(f"Detectados {len(multicollinear)} pares con multicolinealidad (|r| > {threshold})")

        return multicollinear

    def _get_numeric_columns(self, df: pd.DataFrame) -> List[str]:
        """Obtiene columnas numéricas del DataFrame."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        # Excluir columnas que parecen IDs
        filtered_cols = []
        for col in numeric_cols:
            col_lower = col.lower()
            if not any(pattern in col_lower for pattern in ['id', '_id', 'key', '_key']):
                filtered_cols.append(col)

        return filtered_cols

    def __repr__(self) -> str:
        return f"CorrelationAnalyzer(threshold={self.correlation_threshold})"

##RUN##

# In[7]:


"""
Módulo para visualización de correlaciones con Pandas.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Optional, Tuple
import logging


class CorrelationPlotter:
    """Visualiza correlaciones entre variables."""

    def __init__(self, figsize: Tuple = (10, 8)):
        self.figsize = figsize
        self.logger = logging.getLogger(__name__)
        self.logger.info("CorrelationPlotter inicializado")

    def plot_correlation_heatmap(
        self,
        corr_matrix: pd.DataFrame,
        mask_upper: bool = True,
        annot: bool = True,
        cmap: str = "RdBu_r"
    ) -> None:
        """Grafica heatmap de correlación."""
        if corr_matrix.empty:
            self.logger.warning("Matriz de correlación vacía")
            return

        plt.figure(figsize=self.figsize)

        mask = None
        if mask_upper:
            mask = np.triu(np.ones_like(corr_matrix, dtype=bool))

        sns.heatmap(corr_matrix,
                   mask=mask,
                   annot=annot,
                   cmap=cmap,
                   center=0,
                   square=True,
                   fmt='.2f',
                   cbar_kws={"shrink": .8})

        plt.title('Correlation Matrix of Numeric Variables', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.show()

        self.logger.info("Heatmap de correlación completado")

    def plot_correlation_with_target(
        self,
        correlations: pd.Series,
        target_name: str
    ) -> None:
        """Grafica correlaciones con variable objetivo."""
        plt.figure(figsize=self.figsize)

        correlations_sorted = correlations.abs().sort_values(ascending=True)
        colors = ['red' if x < 0 else 'green' for x in correlations[correlations_sorted.index]]

        correlations[correlations_sorted.index].plot(kind='barh', color=colors)
        plt.title(f'Correlations with {target_name}', fontsize=14, fontweight='bold')
        plt.xlabel('Correlation Coefficient')
        plt.axvline(x=0, color='black', linestyle='--', linewidth=0.8)
        plt.tight_layout()
        plt.show()

        self.logger.info(f"Gráfico de correlación con '{target_name}' completado")

    def __repr__(self) -> str:
        return "CorrelationPlotter()"

##RUN##

# ## Profiler

# In[8]:


"""
Módulo para perfilado de datos con Pandas: clasificación de columnas y análisis de cardinalidad.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging


class DataProfiler:
    """
    Perfila el dataset: clasifica columnas, analiza cardinalidad.

    Attributes:
        cardinality_threshold: Umbral para alta cardinalidad (default: 30)
    """

    def __init__(self, cardinality_threshold: int = 30):
        self.cardinality_threshold = cardinality_threshold
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"DataProfiler inicializado (cardinality_threshold={cardinality_threshold})")

    def profile_dataset(self, df: pd.DataFrame) -> Dict:
        """
        Perfil completo del dataset.

        Args:
            df: DataFrame a perfilar

        Returns:
            Dict con perfil completo
        """
        self.logger.info("Perfilando dataset...")

        profile = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'column_classification': self.classify_columns(df),
            'cardinality_analysis': self.analyze_cardinality(df),
            'summary_statistics': self.get_summary_statistics(df)
        }

        self.logger.info("Perfilado completado")
        return profile

    def classify_columns(self, df: pd.DataFrame) -> Dict:
        """
        Clasifica columnas por tipo.

        Returns:
            Dict: {'numeric': [...], 'categorical': [...], 'dates': [...], 'ids': [...],
                   'high_cardinality': [...], 'boolean': [...]}
        """
        numeric_cols = []
        string_cols = []
        date_cols = []
        boolean_cols = []
        id_cols = []
        high_cardinality_cols = []

        # Clasificar por tipo de dato
        for col in df.columns:
            dtype = df[col].dtype

            # Primero verificar si es booleano explícito
            if pd.api.types.is_bool_dtype(dtype):
                boolean_cols.append(col)
            elif pd.api.types.is_numeric_dtype(dtype):
                # Verificar si es una columna numérica que en realidad es booleana (solo 0 y 1)
                unique_vals = df[col].dropna().unique()
                if len(unique_vals) <= 2 and set(unique_vals).issubset({0, 1, 0.0, 1.0}):
                    boolean_cols.append(col)
                else:
                    numeric_cols.append(col)
            elif pd.api.types.is_string_dtype(dtype) or dtype == 'object':
                string_cols.append(col)
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                date_cols.append(col)

        # Detectar IDs
        id_cols = self.detect_id_columns(df)

        # Analizar cardinalidad de columnas string
        for col in string_cols:
            if col in id_cols:
                continue

            distinct_count = df[col].nunique()
            if distinct_count > self.cardinality_threshold:
                high_cardinality_cols.append(col)

        # Categóricas = string - IDs - alta cardinalidad
        categorical_cols = [c for c in string_cols
                          if c not in id_cols and c not in high_cardinality_cols]

        # Filtrar IDs de numeric
        numeric_cols_filtered = [c for c in numeric_cols if c not in id_cols]

        result = {
            'numeric': numeric_cols_filtered,
            'categorical': categorical_cols,
            'dates': date_cols,
            'ids': id_cols,
            'high_cardinality': high_cardinality_cols,
            'boolean': boolean_cols
        }

        self.logger.info(
            f"Clasificación: numeric={len(numeric_cols_filtered)}, "
            f"categorical={len(categorical_cols)}, dates={len(date_cols)}, "
            f"ids={len(id_cols)}, high_card={len(high_cardinality_cols)}, "
            f"boolean={len(boolean_cols)}"
        )

        return result

    def analyze_cardinality(self, df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Analiza cardinalidad de columnas.

        Args:
            df: DataFrame
            columns: Lista de columnas (None = todas las string/object)

        Returns:
            pandas DataFrame con cardinalidad
        """
        if columns is None:
            columns = df.select_dtypes(include=['object', 'string']).columns.tolist()

        cardinality_info = []
        total_count = len(df)

        for col in columns:
            if col not in df.columns:
                continue

            distinct_count = df[col].nunique()
            uniqueness = (distinct_count / total_count * 100) if total_count > 0 else 0

            cardinality_info.append({
                'column': col,
                'distinct_count': distinct_count,
                'total_count': total_count,
                'uniqueness_pct': uniqueness,
                'is_high_cardinality': distinct_count > self.cardinality_threshold
            })

        result = pd.DataFrame(cardinality_info).sort_values('distinct_count', ascending=False)
        return result

    def get_summary_statistics(self, df: pd.DataFrame, numeric_cols: Optional[List[str]] = None) -> pd.DataFrame:
        """Obtiene estadísticas resumidas de columnas numéricas."""
        if numeric_cols is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if not numeric_cols:
            return pd.DataFrame()

        summary = df[numeric_cols].describe()
        return summary

    def detect_id_columns(self, df: pd.DataFrame) -> List[str]:
        """Detecta columnas que son IDs."""
        id_patterns = ['id', 'key', 'code', 'number']
        id_cols = []

        for col in df.columns:
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in id_patterns):
                id_cols.append(col)

        return id_cols

    def detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        """Detecta columnas de fecha."""
        date_cols = []

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_cols.append(col)
            elif 'date' in col.lower() or 'time' in col.lower():
                date_cols.append(col)

        return date_cols

    def __repr__(self) -> str:
        return f"DataProfiler(cardinality_threshold={self.cardinality_threshold})"

##RUN##

# ## Transformer

# In[9]:


"""
Módulo para transformaciones y creación de features derivadas con Pandas.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging


class FeatureTransformer:
    """
    Aplica transformaciones y crea features derivadas con Pandas.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("FeatureTransformer inicializado")

    def create_binary_flags(
        self,
        df: pd.DataFrame,
        conditions: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Crea features binarias (0/1) basadas en condiciones.

        Args:
            df: DataFrame fuente
            conditions: Dict de condiciones {'nombre_feature': 'condición'}

        Returns:
            DataFrame con nuevas features binarias

        Example:
            conditions = {
                'has_cases': 'total_cases > 0',
                'has_open_cases': 'open_cases_count > 0',
                'high_satisfaction': 'avg_survey_rating >= 4.5'
            }
            df_flags = transformer.create_binary_flags(df, conditions)
        """
        result = df.copy()

        for feature_name, condition in conditions.items():
            try:
                result[feature_name] = result.eval(condition).astype(int)
                self.logger.debug(f"Feature binaria creada: {feature_name}")
            except Exception as e:
                self.logger.error(f"Error creando feature '{feature_name}': {e}")

        self.logger.info(f"{len(conditions)} features binarias creadas")

        return result

    def create_ratio_features(
        self,
        df: pd.DataFrame,
        ratios: Dict[str, tuple]
    ) -> pd.DataFrame:
        """
        Crea features de ratio (división entre dos columnas).

        Args:
            df: DataFrame fuente
            ratios: Dict de ratios {'nombre_feature': ('numerador_col', 'denominador_col')}

        Returns:
            DataFrame con nuevas features de ratio

        Example:
            ratios = {
                'case_resolution_rate': ('closed_cases_count', 'total_cases'),
                'high_priority_rate': ('high_priority_cases', 'total_cases')
            }
            df_ratios = transformer.create_ratio_features(df, ratios)
        """
        result = df.copy()

        for feature_name, (numerator_col, denominator_col) in ratios.items():
            if numerator_col not in df.columns:
                self.logger.warning(f"Columna numerador '{numerator_col}' no existe, ignorando")
                continue

            if denominator_col not in df.columns:
                self.logger.warning(f"Columna denominador '{denominator_col}' no existe, ignorando")
                continue

            try:
                result[feature_name] = np.where(
                    df[denominator_col] > 0,
                    df[numerator_col] / df[denominator_col],
                    None
                )
                self.logger.debug(f"Feature ratio creada: {feature_name}")
            except Exception as e:
                self.logger.error(f"Error creando ratio '{feature_name}': {e}")

        self.logger.info(f"{len(ratios)} features de ratio creadas")

        return result

    def create_time_deltas(
        self,
        df: pd.DataFrame,
        date_col: str,
        reference_date: str = "current",
        output_col_suffix: str = "_days_since"
    ) -> pd.DataFrame:
        """
        Crea features de diferencia en días desde una fecha de referencia.

        Args:
            df: DataFrame fuente
            date_col: Columna con la fecha
            reference_date: Fecha de referencia ('current' = fecha actual, o fecha específica)
            output_col_suffix: Sufijo para la columna de salida

        Returns:
            DataFrame con features de días desde

        Example:
            df = transformer.create_time_deltas(df, 'most_recent_case_date')
            # Crea columna: most_recent_case_date_days_since
        """
        if date_col not in df.columns:
            self.logger.warning(f"Columna '{date_col}' no existe")
            return df

        result = df.copy()
        output_col = f"{date_col}{output_col_suffix}"

        try:
            if reference_date == "current":
                ref_date = pd.Timestamp.now()
            else:
                ref_date = pd.to_datetime(reference_date)

            # Convertir a datetime si no lo está
            date_series = pd.to_datetime(result[date_col], errors='coerce')

            result[output_col] = (ref_date - date_series).dt.days

            self.logger.info(f"Feature de tiempo creada: {output_col}")

        except Exception as e:
            self.logger.error(f"Error creando delta de tiempo: {e}")

        return result

    def create_multiple_time_deltas(
        self,
        df: pd.DataFrame,
        date_cols: List[str],
        reference_date: str = "current"
    ) -> pd.DataFrame:
        """
        Crea features de tiempo para múltiples columnas de fecha.

        Args:
            df: DataFrame fuente
            date_cols: Lista de columnas de fecha
            reference_date: Fecha de referencia

        Returns:
            DataFrame con múltiples features de tiempo
        """
        result = df.copy()

        for date_col in date_cols:
            if date_col not in df.columns:
                self.logger.warning(f"Columna '{date_col}' no existe, ignorando")
                continue

            result = self.create_time_deltas(result, date_col, reference_date)

        self.logger.info(f"Features de tiempo creadas para {len(date_cols)} columnas")

        return result

    def apply_custom_transformations(
        self,
        df: pd.DataFrame,
        transformations: List[Dict]
    ) -> pd.DataFrame:
        """
        Aplica transformaciones personalizadas definidas en configuración.

        Args:
            df: DataFrame fuente
            transformations: Lista de transformaciones
                [{'name': 'feature_name', 'formula': 'expression', 'condition': 'optional'}, ...]

        Returns:
            DataFrame con transformaciones aplicadas

        Example:
            transformations = [
                {
                    'name': 'has_cases',
                    'formula': 'total_cases > 0'
                },
                {
                    'name': 'case_resolution_rate',
                    'formula': 'closed_cases_count / total_cases',
                    'condition': 'total_cases > 0'
                }
            ]
            df_transformed = transformer.apply_custom_transformations(df, transformations)
        """
        result = df.copy()

        for transform in transformations:
            name = transform.get('name')
            formula = transform.get('formula')
            condition = transform.get('condition')

            if not name or not formula:
                self.logger.warning(f"Transformación incompleta, ignorando: {transform}")
                continue

            try:
                # Aplicar transformación
                if condition:
                    # Con condición
                    mask = result.eval(condition)
                    result.loc[mask, name] = result.loc[mask].eval(formula)
                    result.loc[~mask, name] = None
                else:
                    # Sin condición
                    result[name] = result.eval(formula)

                self.logger.debug(f"Transformación aplicada: {name}")

            except Exception as e:
                self.logger.error(f"Error aplicando transformación '{name}': {e}")

        self.logger.info(f"{len(transformations)} transformaciones aplicadas")

        return result

    def create_categorical_encodings(
        self,
        df: pd.DataFrame,
        categorical_cols: List[str],
        encoding_type: str = "onehot"
    ) -> pd.DataFrame:
        """
        Crea encodings para columnas categóricas.

        Args:
            df: DataFrame fuente
            categorical_cols: Lista de columnas categóricas
            encoding_type: Tipo de encoding ('onehot', 'label')

        Returns:
            DataFrame con encodings

        Note:
            Por ahora solo implementa one-hot encoding simple.
        """
        if encoding_type != "onehot":
            self.logger.warning(
                f"Encoding type '{encoding_type}' no soportado aún. "
                f"Usando 'onehot'"
            )

        result = df.copy()

        for col in categorical_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            # Obtener valores únicos
            unique_values = df[col].dropna().unique()

            # Crear columna binaria para cada valor
            for value in unique_values:
                safe_value = str(value).replace(' ', '_').replace('-', '_').lower()
                encoded_col = f"{col}_is_{safe_value}"

                result[encoded_col] = (df[col] == value).astype(int)

            self.logger.info(f"One-hot encoding creado para '{col}': {len(unique_values)} valores")

        return result

    def create_interaction_features(
        self,
        df: pd.DataFrame,
        interactions: List[tuple]
    ) -> pd.DataFrame:
        """
        Crea features de interacción (producto de dos columnas).

        Args:
            df: DataFrame fuente
            interactions: Lista de tuplas (col1, col2, nombre_output)

        Returns:
            DataFrame con features de interacción
        """
        result = df.copy()

        for col1, col2, output_name in interactions:
            if col1 not in df.columns or col2 not in df.columns:
                self.logger.warning(
                    f"Columnas '{col1}' o '{col2}' no existen, ignorando interacción"
                )
                continue

            try:
                result[output_name] = df[col1] * df[col2]
                self.logger.debug(f"Feature de interacción creada: {output_name}")
            except Exception as e:
                self.logger.error(f"Error creando interacción '{output_name}': {e}")

        self.logger.info(f"{len(interactions)} features de interacción creadas")

        return result

    def create_count_features(
        self,
        df: pd.DataFrame,
        group_col: str,
        categorical_cols: List[str]
    ) -> pd.DataFrame:
        """
        Crea features de conteo para columnas categóricas.

        Args:
            df: DataFrame fuente
            group_col: Columna para agrupar
            categorical_cols: Lista de columnas categóricas a contar

        Returns:
            DataFrame con conteos por categoría

        Example:
            df_counts = transformer.create_count_features(
                df, 'uniqueId', ['status', 'priority', 'category']
            )
        """
        if group_col not in df.columns:
            self.logger.warning(f"Columna de grupo '{group_col}' no existe")
            return pd.DataFrame()

        result = df[[group_col]].drop_duplicates()

        for col in categorical_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            # Crear crosstab
            counts = pd.crosstab(df[group_col], df[col])

            # Renombrar columnas
            counts.columns = [f'{col}_{str(val).replace(" ", "_").lower()}_count'
                            for val in counts.columns]

            # Merge con resultado
            result = result.merge(counts, left_on=group_col, right_index=True, how='left')

        self.logger.info(f"Features de conteo creadas para {len(categorical_cols)} columnas")

        return result

    def create_time_features(
        self,
        df: pd.DataFrame,
        group_col: str,
        date_cols: List[str]
    ) -> pd.DataFrame:
        """
        Crea features temporales (min, max, count) para columnas de fechas.

        Args:
            df: DataFrame fuente
            group_col: Columna para agrupar
            date_cols: Lista de columnas de fecha

        Returns:
            DataFrame con features temporales
        """
        if group_col not in df.columns:
            self.logger.warning(f"Columna de grupo '{group_col}' no existe")
            return pd.DataFrame()

        agg_dict = {}

        for col in date_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            agg_dict[col] = ['min', 'max', 'count']

        if not agg_dict:
            return df[[group_col]].drop_duplicates()

        result = df.groupby(group_col).agg(agg_dict).reset_index()

        # Aplanar columnas multinivel
        result.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                         for col in result.columns.values]

        # Renombrar según convención
        rename_map = {}
        for col in date_cols:
            rename_map[f'{col}_min'] = f'{col}_first'
            rename_map[f'{col}_max'] = f'{col}_last'

        result = result.rename(columns=rename_map)

        self.logger.info(f"Features temporales creadas para {len(date_cols)} columnas")

        return result

    def format_date_columns(
        self,
        df: pd.DataFrame,
        date_cols: List[str],
        format_string: str = "%Y-%m-%d",
        create_new_columns: bool = False,
        suffix: str = "_formatted"
    ) -> pd.DataFrame:
        """
        Formatea columnas de fecha para mejor legibilidad.

        Args:
            df: DataFrame fuente
            date_cols: Lista de columnas de fecha a formatear
            format_string: Formato de salida (default: "%Y-%m-%d")
                Opciones comunes:
                - "%Y-%m-%d" -> "2024-01-15" (ISO estándar)
                - "%d/%m/%Y" -> "15/01/2024" (formato latino)
                - "%d-%b-%Y" -> "15-Jan-2024" (con mes abreviado)
            create_new_columns: Si True, crea nuevas columnas; si False, modifica in-place
            suffix: Sufijo para nuevas columnas (solo si create_new_columns=True)

        Returns:
            DataFrame con fechas formateadas

        Example:
            df = transformer.format_date_columns(
                df,
                date_cols=['created_date', 'modified_date'],
                format_string="%d/%m/%Y",
                create_new_columns=True
            )
        """
        result = df.copy()

        for col in date_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            try:
                date_series = pd.to_datetime(result[col], errors='coerce')
                formatted = date_series.dt.strftime(format_string)

                if create_new_columns:
                    result[f"{col}{suffix}"] = formatted
                    self.logger.debug(f"Columna formateada creada: {col}{suffix}")
                else:
                    result[col] = formatted
                    self.logger.debug(f"Columna formateada: {col}")

            except Exception as e:
                self.logger.error(f"Error formateando columna '{col}': {e}")

        self.logger.info(f"{len(date_cols)} columnas de fecha formateadas")
        return result

    def strip_time_from_dates(
        self,
        df: pd.DataFrame,
        date_cols: List[str],
        create_new_columns: bool = False,
        suffix: str = "_date_only"
    ) -> pd.DataFrame:
        """
        Elimina el componente de tiempo de columnas datetime, dejando solo la fecha.

        Args:
            df: DataFrame fuente
            date_cols: Lista de columnas datetime a procesar
            create_new_columns: Si True, crea nuevas columnas; si False, modifica in-place
            suffix: Sufijo para nuevas columnas (solo si create_new_columns=True)

        Returns:
            DataFrame con fechas sin componente de tiempo

        Example:
            # Antes: "2024-01-15 14:30:00"
            # Después: "2024-01-15"
            df = transformer.strip_time_from_dates(df, ['created_at', 'updated_at'])
        """
        result = df.copy()

        for col in date_cols:
            if col not in df.columns:
                self.logger.warning(f"Columna '{col}' no existe, ignorando")
                continue

            try:
                date_series = pd.to_datetime(result[col], errors='coerce')
                date_only = date_series.dt.normalize()  # Mantiene datetime pero a medianoche

                if create_new_columns:
                    result[f"{col}{suffix}"] = date_only
                    self.logger.debug(f"Columna date-only creada: {col}{suffix}")
                else:
                    result[col] = date_only
                    self.logger.debug(f"Tiempo eliminado de: {col}")

            except Exception as e:
                self.logger.error(f"Error procesando columna '{col}': {e}")

        self.logger.info(f"Tiempo eliminado de {len(date_cols)} columnas")
        return result

    def __repr__(self) -> str:
        """Representación string del transformador."""
        return "FeatureTransformer()"

##RUN##

# ## Distributions

# In[10]:


"""
Módulo para visualización de distribuciones de datos con Pandas.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Optional, Tuple
import logging


class DistributionPlotter:
    """Crea visualizaciones de distribuciones de datos."""

    def __init__(self, style: str = "default", figsize: Tuple = (12, 8)):
        self.style = style
        self.figsize = figsize
        self.logger = logging.getLogger(__name__)

        plt.style.use(self.style)
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = self.figsize

        self.logger.info(f"DistributionPlotter inicializado (style={style})")

    def plot_categorical_distributions(
        self,
        df: pd.DataFrame,
        categorical_cols: List[str],
        max_categories: Optional[int] = None,
        n_cols: int = 3
    ) -> None:
        """Grafica distribuciones de columnas categóricas."""
        if not categorical_cols:
            self.logger.warning("No hay columnas categóricas para graficar")
            return

        self.logger.info(f"Graficando {len(categorical_cols)} columnas categóricas...")

        n_categorical = len(categorical_cols)
        n_rows = (n_categorical + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(8*n_cols, 6*n_rows))
        axes = axes.flatten() if n_rows > 1 or n_cols > 1 else [axes]

        for i, col_name in enumerate(categorical_cols):
            value_counts = df[col_name].value_counts()

            if max_categories:
                value_counts = value_counts.head(max_categories)

            ax = axes[i]
            value_counts.plot(kind='bar', ax=ax,
                            color=sns.color_palette("husl", len(value_counts)))
            ax.set_title(f'Distribution of {col_name}', fontsize=12, fontweight='bold')
            ax.set_xlabel(col_name)
            ax.set_ylabel('Count')
            ax.tick_params(axis='x', rotation=45)

            for j, v in enumerate(value_counts.values):
                ax.text(j, v + 0.01*max(value_counts.values), str(v),
                       ha='center', va='bottom', fontweight='bold')

        for i in range(n_categorical, len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        plt.show()

        self.logger.info("Gráficos categóricos completados")

    def plot_numeric_distributions(
        self,
        df: pd.DataFrame,
        numeric_cols: List[str],
        bins: int = 30,
        n_cols: int = 3
    ) -> None:
        """Grafica histogramas de columnas numéricas."""
        if not numeric_cols:
            self.logger.warning("No hay columnas numéricas para graficar")
            return

        # Filtrar columnas booleanas que causan problemas con NumPy 2.0
        valid_numeric_cols = []
        for col in numeric_cols:
            if col in df.columns:
                # Excluir columnas booleanas o con solo 2 valores únicos (0,1)
                if df[col].dtype == bool or pd.api.types.is_bool_dtype(df[col]):
                    self.logger.info(f"Saltando columna booleana: {col}")
                    continue
                # Convertir a numérico si es posible
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    valid_numeric_cols.append(col)
                except:
                    self.logger.warning(f"No se pudo convertir {col} a numérico")

        if not valid_numeric_cols:
            self.logger.warning("No hay columnas numéricas válidas para graficar después del filtrado")
            return

        self.logger.info(f"Graficando {len(valid_numeric_cols)} columnas numéricas...")

        n_numeric = len(valid_numeric_cols)
        n_rows = (n_numeric + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(5*n_cols, 4*n_rows))
        axes = axes.flatten() if n_rows > 1 or n_cols > 1 else [axes]

        for i, col_name in enumerate(valid_numeric_cols):
            col_data = df[col_name].dropna()

            mean_val = col_data.mean()
            std_val = col_data.std()
            median_val = col_data.median()

            ax = axes[i]
            # Usar ax.hist() en lugar de col_data.hist() para evitar problemas con NumPy 2.0
            ax.hist(col_data, bins=bins, alpha=0.7,
                   color=sns.color_palette("husl", n_colors=35)[i % 10])
            ax.set_title(f'Distribution of {col_name}', fontsize=12, fontweight='bold')
            ax.set_xlabel(col_name)
            ax.set_ylabel('Frequency')
            ax.grid(True, alpha=0.3)

            stats_text = f'Mean: {mean_val:.2f}\nStd: {std_val:.2f}\nMedian: {median_val:.2f}'
            ax.text(0.95, 0.95, stats_text, transform=ax.transAxes,
                   verticalalignment='top', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        for i in range(len(valid_numeric_cols), len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        plt.show()

        self.logger.info("Histogramas completados")

    def plot_top_n_categories(
        self,
        df: pd.DataFrame,
        high_cardinality_cols: List[str],
        top_n: int = 10,
        n_cols: int = 3
    ) -> None:
        """Grafica top N valores de columnas con alta cardinalidad."""
        if not high_cardinality_cols:
            self.logger.info("No hay columnas de alta cardinalidad para graficar")
            return

        self.logger.info(f"Graficando top {top_n} de {len(high_cardinality_cols)} columnas...")

        n_categorical = len(high_cardinality_cols)
        n_rows = (n_categorical + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(8*n_cols, 6*n_rows))
        axes = axes.flatten() if n_rows > 1 or n_cols > 1 else [axes]

        for i, col_name in enumerate(high_cardinality_cols):
            value_counts = df[col_name].value_counts().head(top_n)

            ax = axes[i]
            value_counts.plot(kind='bar', ax=ax,
                            color=sns.color_palette("husl", len(value_counts)))
            ax.set_title(f'Top {top_n} Values in {col_name}', fontsize=12, fontweight='bold')
            ax.set_xlabel(col_name)
            ax.set_ylabel('Count')
            ax.tick_params(axis='x', rotation=45)

            for j, v in enumerate(value_counts.values):
                ax.text(j, v + 0.01*max(value_counts.values), str(v),
                       ha='center', va='bottom', fontweight='bold')

        for i in range(n_categorical, len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        plt.show()

        self.logger.info("Gráficos de alta cardinalidad completados")

    def plot_date_distribution_by_month(
        self,
        df: pd.DataFrame,
        date_cols: List[str],
        aggregate_years: bool = False,
        show_counts: bool = True,
        n_cols: int = 2
    ) -> None:
        """
        Crea histogramas mostrando la distribución de fechas por mes.

        Args:
            df: DataFrame fuente
            date_cols: Lista de columnas de fecha
            aggregate_years: Si False, muestra año-mes individual (default);
                           Si True, agrupa todos los años por mes (Ene-Dic)
            show_counts: Mostrar conteos encima de las barras
            n_cols: Número de columnas en el grid de gráficos

        Returns:
            None (muestra gráficos)

        Example:
            plotter.plot_date_distribution_by_month(
                df,
                date_cols=['created_date', 'closed_date'],
                aggregate_years=False  # Muestra 2023-01, 2023-02, etc.
            )
        """
        if not date_cols:
            self.logger.warning("No hay columnas de fecha para graficar")
            return

        valid_date_cols = [col for col in date_cols if col in df.columns]
        if not valid_date_cols:
            self.logger.warning("Ninguna de las columnas especificadas existe")
            return

        self.logger.info(f"Graficando distribución mensual de {len(valid_date_cols)} columnas...")

        n_plots = len(valid_date_cols)
        n_rows = (n_plots + n_cols - 1) // n_cols

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(7*n_cols, 5*n_rows))
        if n_rows == 1 and n_cols == 1:
            axes = [axes]
        else:
            axes = axes.flatten()

        month_names = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
                       'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

        for i, col in enumerate(valid_date_cols):
            ax = axes[i]

            try:
                date_series = pd.to_datetime(df[col], errors='coerce').dropna()

                if len(date_series) == 0:
                    ax.set_title(f'{col} - Sin datos', fontsize=12)
                    continue

                if aggregate_years:
                    # Agrupar solo por mes (1-12)
                    month_counts = date_series.dt.month.value_counts().sort_index()
                    x_labels = [month_names[m-1] for m in month_counts.index]

                    bars = ax.bar(x_labels, month_counts.values,
                                color=sns.color_palette("husl", 12))
                    ax.set_xlabel('Mes')
                else:
                    # Agrupar por año-mes
                    year_month = date_series.dt.to_period('M')
                    month_counts = year_month.value_counts().sort_index()

                    x_positions = range(len(month_counts))
                    bars = ax.bar(x_positions, month_counts.values,
                                color=sns.color_palette("husl", min(len(month_counts), 24)))

                    # Mostrar etiquetas cada N meses para no saturar
                    step = max(1, len(month_counts) // 12)
                    tick_positions = list(range(0, len(month_counts), step))
                    tick_labels = [str(month_counts.index[j]) for j in tick_positions]
                    ax.set_xticks(tick_positions)
                    ax.set_xticklabels(tick_labels, rotation=45, ha='right')
                    ax.set_xlabel('Año-Mes')

                ax.set_ylabel('Frecuencia')
                ax.set_title(f'Distribución Mensual: {col}', fontsize=12, fontweight='bold')
                ax.grid(axis='y', alpha=0.3)

                # Mostrar conteos encima de las barras
                if show_counts and len(bars) <= 24:  # Solo si no hay muchas barras
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                               f'{int(height)}',
                               ha='center', va='bottom', fontsize=8)

            except Exception as e:
                self.logger.error(f"Error graficando '{col}': {e}")
                ax.set_title(f'{col} - Error', fontsize=12)

        # Ocultar ejes vacíos
        for i in range(len(valid_date_cols), len(axes)):
            axes[i].set_visible(False)

        plt.tight_layout()
        plt.show()

        self.logger.info("Gráficos de distribución mensual completados")

    def __repr__(self) -> str:
        return f"DistributionPlotter(style='{self.style}')"

##RUN##

# ## Nulls

# In[11]:


"""
Módulo para visualización de análisis de nulos con Pandas.
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import Dict, Tuple
import logging


class NullAnalysisPlotter:
    """Crea visualizaciones para análisis de valores nulos."""

    def __init__(self, figsize: Tuple = (16, 6)):
        self.figsize = figsize
        self.logger = logging.getLogger(__name__)
        self.logger.info("NullAnalysisPlotter inicializado")

    def plot_null_percentages(self, null_analysis: pd.DataFrame) -> None:
        """Grafica porcentajes de nulos por columna."""
        columns_with_nulls = null_analysis[null_analysis['null_count'] > 0]

        if columns_with_nulls.empty:
            self.logger.info("No hay columnas con nulos para graficar")
            return

        columns_with_nulls_sorted = columns_with_nulls.sort_values('null_percentage', ascending=True)

        plt.figure(figsize=self.figsize)
        plt.barh(range(len(columns_with_nulls_sorted)), columns_with_nulls_sorted['null_percentage'])
        plt.yticks(range(len(columns_with_nulls_sorted)), columns_with_nulls_sorted['column'])
        plt.xlabel('Porcentaje de Nulos (%)', fontweight='bold')
        plt.title('Porcentaje de Valores Nulos por Columna', fontweight='bold', fontsize=14)
        plt.grid(axis='x', alpha=0.3)

        for i, v in enumerate(columns_with_nulls_sorted['null_percentage']):
            plt.text(v + 1, i, f'{v:.1f}%', va='center')

        plt.tight_layout()
        plt.show()

        self.logger.info("Gráfico de porcentajes de nulos completado")

    def plot_null_severity_pie(self, null_severity: Dict) -> None:
        """Grafica pie chart de severidad de nulos."""
        severity_counts = [
            len(null_severity['critical']),
            len(null_severity['high']),
            len(null_severity['moderate']),
            len(null_severity['low'])
        ]

        severity_labels = [
            f'Crítico (>50%): {severity_counts[0]} cols',
            f'Alto (20-50%): {severity_counts[1]} cols',
            f'Moderado (5-20%): {severity_counts[2]} cols',
            f'Bajo (<5%): {severity_counts[3]} cols'
        ]

        colors = ['#d62728', '#ff7f0e', '#ffbb78', '#2ca02c']

        # Filtrar solo categorías con valores
        severity_counts_filtered = [c for c in severity_counts if c > 0]
        severity_labels_filtered = [l for i, l in enumerate(severity_labels) if severity_counts[i] > 0]
        colors_filtered = [c for i, c in enumerate(colors) if severity_counts[i] > 0]

        if not severity_counts_filtered:
            self.logger.info("No hay columnas con nulos para clasificar por severidad")
            return

        plt.figure(figsize=(10, 8))
        plt.pie(severity_counts_filtered, labels=severity_labels_filtered, colors=colors_filtered,
               autopct='%1.1f%%', startangle=90)
        plt.title('Clasificación de Columnas por Severidad de Nulos', fontweight='bold', fontsize=14)
        plt.tight_layout()
        plt.show()

        self.logger.info("Pie chart de severidad completado")

    def __repr__(self) -> str:
        return "NullAnalysisPlotter()"

##RUN##

# ## Test

# In[1]:


from deltalake import DeltaTable, write_deltalake
table_path = 'abfss://Dev_Vensure_Gold@onelake.dfs.fabric.microsoft.com/lakehouse_gold.Lakehouse/Tables/dbo' 
storage_options = {"bearer_token": notebookutils.credentials.getToken('storage'), "use_fabric_endpoint": "true"}

##RUN##

# In[2]:


df_cm = DeltaTable(table_path + "/Client_Master", storage_options=storage_options).to_pandas()
df_cs = DeltaTable(table_path + "/Case_Survey", storage_options=storage_options).to_pandas()

df_clients_filtered = df_cm[df_cm['isCurrent'] == 1]
print(f"  Clientes después de filtro: {len(df_clients_filtered)}")

##RUN##

# In[14]:


# 2. Merge
merger = DataMerger(join_strategy='left')

# Comparar estrategias de join
print("  Comparando estrategias de join...")
comparison = merger.compare_join_strategies(df_clients_filtered, df_cs, on='uniqueId')

# Ejecutar merge
df_merged = merger.merge(
    df_clients_filtered,
    df_cs,
    on='uniqueId',
    how='left',
    drop_duplicate_columns=True
)

# Validar calidad del merge
validation = merger.validate_merge_quality(
    df_merged, df_clients_filtered, df_cs, 'uniqueId'
)
print(f"  Match rate: {validation['match_rate']:.2f}%")
print(f"  Claves distintas merged: {validation['distinct_keys_merged']}")

##RUN##

# In[15]:


# 3. Feature Engineering - Agregación
aggregator = FeatureAggregator()

# Crear features agregadas de casos
df_features = aggregator.create_case_features(df_cs, group_by='uniqueId')
print(f"  Features de casos creadas: {len(df_features.columns) - 1}")

# Crear features de conteo
df_count_features = aggregator.create_count_features(
    df_cs,
    group_col='uniqueId',
    categorical_cols=['status', 'priority', 'category']
)
print(f"  Features de conteo creadas: {len(df_count_features.columns) - 1}")

# Crear features temporales
df_time_features = aggregator.create_time_features(
    df_cs,
    group_col='uniqueId',
    date_cols=['caseDateCreated']
)
print(f"  Features temporales creadas: {len(df_time_features.columns) - 1}")

# Join features con clientes
df_final_2 = pd.merge(df_clients_filtered, df_features, on='uniqueId', how='left')

# Rellenar nulos para clientes sin casos
df_final_2 = aggregator.fill_missing_aggregations(df_final_2)

print(f"  DataFrame final: {df_final_2.shape}")

##RUN##

# In[16]:


# 4. Feature Engineering - Transformación
transformer = FeatureTransformer()

# Crear features binarias
binary_conditions = {
    'has_cases': 'total_cases > 0',
    'has_open_cases': 'open_cases_count > 0',
    'high_satisfaction': 'avg_survey_rating >= 4.0'
}
df_final_2 = transformer.create_binary_flags(df_final_2, binary_conditions)
print(f"  Features binarias creadas: {len(binary_conditions)}")

# Crear features de ratio
ratios = {
    'case_resolution_rate': ('closed_cases_count', 'total_cases'),
    'high_priority_rate': ('high_priority_cases', 'total_cases')
}
df_final_2 = transformer.create_ratio_features(df_final_2, ratios)
print(f"  Features de ratio creadas: {len(ratios)}")

# Crear features de tiempo
if 'most_recent_case_date' in df_final_2.columns:
    df_final_2 = transformer.create_time_deltas(
        df_final_2,
        'most_recent_case_date',
        reference_date='current'
    )
    print(f"  Features temporales creadas")

# Crear features de interacción
interactions = [
    ('total_cases', 'avg_survey_rating', 'cases_x_rating'),
]
df_final_2 = transformer.create_interaction_features(df_final_2, interactions)
print(f"  Features de interacción creadas: {len(interactions)}")

print(f"\n  Total columnas: {len(df_final_2.columns)}")

##RUN##

# In[17]:


# 5. Análisis de calidad
analyzer = DataQualityAnalyzer()

# Análisis de nulos
null_analysis = analyzer.analyze_nulls(df_final_2)
print("\nAnálisis de nulos:")
cols_with_nulls = null_analysis[null_analysis['null_count'] > 0]
if not cols_with_nulls.empty:
    print(cols_with_nulls)
else:
    print("  No hay columnas con nulos")

# Clasificación de severidad
null_severity = analyzer.classify_null_severity(df_final_2)
print(f"\nSeveridad de nulos:")
print(f"  Crítico: {len(null_severity['critical'])}")
print(f"  Alto: {len(null_severity['high'])}")
print(f"  Moderado: {len(null_severity['moderate'])}")
print(f"  Bajo: {len(null_severity['low'])}")

# Score de completitud
completeness = analyzer.calculate_completeness_score(df_final_2)
print(f"\nCompleteness score: {completeness:.2f}%")

# Sugerencias de estrategias
if null_analysis['null_count'].sum() > 0:
    strategies = analyzer.suggest_null_strategies(df_final_2)
    print("\nEstrategias sugeridas para nulos:")
    print(strategies)

##RUN##

# In[18]:


# 6. Perfilado de datos
profiler = DataProfiler(cardinality_threshold=30)

# Perfil completo
profile = profiler.profile_dataset(df_final_2)

print(f"\nResumen del dataset:")
print(f"  Filas: {profile['row_count']:,}")
print(f"  Columnas: {profile['column_count']}")

# Clasificación de columnas
col_classification = profile['column_classification']
print(f"\nClasificación de columnas:")
print(f"  Numéricas: {len(col_classification['numeric'])} - {col_classification['numeric']}")
print(f"  Categóricas: {len(col_classification['categorical'])} - {col_classification['categorical']}")
print(f"  Fechas: {len(col_classification['dates'])} - {col_classification['dates']}")
print(f"  IDs: {len(col_classification['ids'])} - {col_classification['ids']}")
print(f"  Alta cardinalidad: {len(col_classification['high_cardinality'])} - {col_classification['high_cardinality']}")
print(f"  Booleanas: {len(col_classification['boolean'])} - {col_classification['boolean']}")

# Análisis de cardinalidad
if col_classification['categorical']:
    cardinality_analysis = profiler.analyze_cardinality(df_final_2, col_classification['categorical'])
    print("\nAnálisis de cardinalidad:")
    print(cardinality_analysis)

# Estadísticas resumidas
if col_classification['numeric']:
    print("\nEstadísticas de columnas numéricas:")
    print(profile['summary_statistics'])

##RUN##

# In[19]:


# 7. Análisis de correlaciones
corr_analyzer = CorrelationAnalyzer(correlation_threshold=0.7)

# Calcular matriz de correlación
if len(col_classification['numeric']) >= 2:
    corr_matrix = corr_analyzer.calculate_correlation_matrix(df_final_2, col_classification['numeric'])

    print("\nMatriz de correlación:")
    print(corr_matrix)

    # Encontrar correlaciones fuertes
    strong_corrs = corr_analyzer.find_strong_correlations(corr_matrix, threshold=0.7)
    if strong_corrs:
        print(f"\nCorrelaciones fuertes encontradas (|r| > 0.7):")
        for var1, var2, corr in strong_corrs:
            print(f"  {var1} <-> {var2}: {corr:.3f}")
    else:
        print("\nNo se encontraron correlaciones fuertes (|r| > 0.7)")

    # Detectar multicolinealidad
    multicollinear = corr_analyzer.detect_multicollinearity(df_final_2, threshold=0.9)
    if multicollinear:
        print(f"\nMulticolinealidad detectada (|r| > 0.9):")
        for var1, var2, corr in multicollinear:
            print(f"  {var1} <-> {var2}: {corr:.3f}")
    else:
        print("\nNo se detectó multicolinealidad (|r| > 0.9)")

    # Analizar correlación con variable objetivo
    if 'avg_survey_rating' in col_classification['numeric']:
        target_corrs = corr_analyzer.analyze_correlation_with_target(df_final_2, 'avg_survey_rating')
        print(f"\nCorrelaciones con avg_survey_rating:")
        print(target_corrs)
else:
    print("\nNo hay suficientes columnas numéricas para análisis de correlación")

##RUN##

# In[20]:


# Inicializar plotters
dist_plotter = DistributionPlotter(figsize=(12, 8))
corr_plotter = CorrelationPlotter(figsize=(10, 8))
null_plotter = NullAnalysisPlotter(figsize=(16, 6))

# Gráficos de distribuciones categóricas
if col_classification['categorical']:
    print("\nGenerando gráficos de distribuciones categóricas...")
    dist_plotter.plot_categorical_distributions(
        df_final_2,
        col_classification['categorical'][:6],  # Primeras 6 columnas
        max_categories=10
    )

# Gráficos de distribuciones numéricas
if col_classification['numeric']:
    print("\nGenerando histogramas de variables numéricas...")
    dist_plotter.plot_numeric_distributions(
        df_final_2,
        col_classification['numeric'][:6],  # Primeras 6 columnas
        bins=20
    )

# Heatmap de correlaciones
if len(col_classification['numeric']) >= 2:
    print("\nGenerando heatmap de correlaciones...")
    corr_plotter.plot_correlation_heatmap(
        corr_matrix,
        mask_upper=True,
        annot=True,
        cmap="RdBu_r"
    )

    # Correlaciones con variable objetivo
    if 'avg_survey_rating' in col_classification['numeric']:
        print("\nGenerando gráfico de correlaciones con avg_survey_rating...")
        target_corrs_series = corr_matrix['avg_survey_rating'].drop('avg_survey_rating')
        corr_plotter.plot_correlation_with_target(
            target_corrs_series,
            'avg_survey_rating'
        )

# Gráficos de análisis de nulos
if null_analysis['null_count'].sum() > 0:
    print("\nGenerando gráficos de análisis de nulos...")
    null_plotter.plot_null_percentages(null_analysis)
    null_plotter.plot_null_severity_pie(null_severity)

##RUN##

# ## Pipeline

# In[24]:


"""
Pipeline simplificado para Pandas.
"""

import pandas as pd
from typing import Dict, Optional
import logging

class EDAPipeline:
    """
    Pipeline EDA simplificado para Pandas.

    Ejemplo:
        pipeline = EDAPipeline()
        pipeline.load_csv('data/clients.csv', alias='clients')
        pipeline.load_csv('data/cases.csv', alias='cases')

        results = pipeline.run_full_pipeline(
            merge_on='uniqueId',
            merge_how='left'
        )
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data = {}
        self.results = {}
        self.merger = DataMerger()
        self.aggregator = FeatureAggregator()
        self.quality_analyzer = DataQualityAnalyzer()

    def load_csv(self, file_path: str, alias: str = None, **kwargs):
        """Carga un archivo CSV."""
        alias = alias or file_path
        self.data[alias] = pd.read_csv(file_path, **kwargs)
        self.logger.info(f"Cargado {alias}: {len(self.data[alias]):,} filas")

    def load_dataframe(self, df: pd.DataFrame, alias: str):
        """Carga un DataFrame existente."""
        self.data[alias] = df
        self.logger.info(f"Cargado {alias}: {len(df):,} filas")

    def run_full_pipeline(
        self,
        merge_on: str = 'uniqueId',
        merge_how: str = 'left',
        create_features: bool = True
    ) -> Dict:
        """Ejecuta pipeline completo."""
        print("="*80)
        print("PIPELINE EDA (PANDAS)")
        print("="*80)

        if len(self.data) < 2:
            print("\n[1/4] Un solo dataset, saltando merge...")
            df_merged = list(self.data.values())[0]
        elif len(self.data) == 2:
            print("\n[1/4] Merge de 2 datasets...")
            datasets = list(self.data.values())
            df_merged = self.merger.merge(datasets[0], datasets[1], on=merge_on, how=merge_how)
            print(f"  Resultado: {len(df_merged):,} filas")
        else:
            # Soporte para N datasets (más de 2)
            print(f"\n[1/4] Merge múltiple de {len(self.data)} datasets...")
            datasets = list(self.data.values())
            df_merged = self.merger.merge_multiple(
                dataframes=datasets,
                on=merge_on,
                how=merge_how,
                validate_intermediate=True
            )
            print(f"  Resultado: {len(df_merged):,} filas")

        self.results['merged_dataframe'] = df_merged

        if create_features and len(self.data) >= 2:
            print("\n[2/4] Feature engineering...")
            detail_df = list(self.data.values())[1]
            df_features = self.aggregator.create_case_features(detail_df, group_by=merge_on)

            df_final = pd.merge(df_merged, df_features, on=merge_on, how='left')
            df_final = self.aggregator.fill_missing_aggregations(df_final)

            print(f"  Features creadas: {len(df_features.columns)-1}")
        else:
            df_final = df_merged

        self.results['feature_engineered_dataframe'] = df_final

        print("\n[3/4] Análisis de calidad...")
        null_analysis = self.quality_analyzer.analyze_nulls(df_final)
        null_severity = self.quality_analyzer.classify_null_severity(df_final)
        completeness = self.quality_analyzer.calculate_completeness_score(df_final)

        quality_report = {
            'null_analysis': null_analysis,
            'null_severity': null_severity,
            'completeness_score': completeness
        }

        self.results['quality_report'] = quality_report

        print(f"  Completeness: {completeness:.2f}%")
        print(f"  Nulos críticos: {len(null_severity['critical'])}")

        print("\n[4/4] Resumen...")
        print(f"  Total filas: {len(df_final):,}")
        print(f"  Total columnas: {len(df_final.columns)}")

        print("\n" + "="*80)
        print("PIPELINE COMPLETADO")
        print("="*80)

        return self.results

    def get_dataframe(self, name: str = 'feature_engineered_dataframe') -> pd.DataFrame:
        """Obtiene DataFrame de resultados."""
        return self.results.get(name)

    def __repr__(self) -> str:
        return f"EDAPipeline(datasets={list(self.data.keys())})"

##RUN##

# In[25]:


pipeline = EDAPipeline()

# Cargar DataFrames al pipeline
pipeline.load_dataframe(df_cm, alias='clients')
pipeline.load_dataframe(df_cs, alias='cases')

# Ejecutar pipeline completo
results = pipeline.run_full_pipeline(
    merge_on='uniqueId',
    merge_how='left',
    create_features=True
)

# Acceder a resultados
df_final = results['feature_engineered_dataframe']
quality_report = results['quality_report']

print("\n" + "="*80)
print("RESULTADOS DEL PIPELINE")
print("="*80)

print(f"\nDataFrame final:")
print(f"  Filas: {len(df_final):,}")
print(f"  Columnas: {len(df_final.columns)}")
print(f"  Completeness: {quality_report['completeness_score']:.2f}%")
print(f"  Nulos críticos: {len(quality_report['null_severity']['critical'])}")

print("\nColumnas generadas:")
print(df_final.columns.tolist())

print("\nMuestra de datos:")
print(df_final.head())

print("\nEstadísticas de features agregadas:")
feature_cols = [col for col in df_final.columns if any(x in col for x in ['cases', 'rating', 'aging', 'priority'])]
if feature_cols:
    print(df_final[feature_cols].describe())

##RUN##
