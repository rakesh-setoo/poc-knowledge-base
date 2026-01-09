import io
import pandas as pd
from app.parsers.base import BaseParser


class ExcelParser(BaseParser):
    
    @property
    def supported_extensions(self) -> list[str]:
        return ['.xlsx', '.xls']
    
    @property
    def name(self) -> str:
        return "Excel"
    
    async def parse(
        self, 
        content: bytes, 
        filename: str,
        progress_callback: callable = None
    ) -> pd.DataFrame:
        try:
            if progress_callback:
                await progress_callback(35, "35% - Reading Excel structure...")
            
            try:
                df_raw = pd.read_excel(io.BytesIO(content), header=None, engine='calamine')
                engine = 'calamine'
            except Exception:
                df_raw = pd.read_excel(io.BytesIO(content), header=None, engine='openpyxl')
                engine = 'openpyxl'
            
            if progress_callback:
                await progress_callback(42, f"42% - Detecting header row (using {engine})...")
            
            header_row = self._detect_header_row(df_raw)
            
            if progress_callback:
                await progress_callback(48, "48% - Parsing Excel data...")
            
            # Re-read with detected header using same engine
            df = pd.read_excel(io.BytesIO(content), header=header_row, engine=engine)
            
            df = self._clean_columns(df)
            
            return df
            
        except Exception as e:
            raise ValueError(f"Failed to parse Excel file: {str(e)}")
    
    def _detect_header_row(self, df_raw: pd.DataFrame) -> int:
        header_row = 0
        max_valid_cols = 0
        
        for i in range(min(10, len(df_raw))):
            row = df_raw.iloc[i]
            valid_cols = sum(
                1 for v in row 
                if pd.notna(v) and isinstance(v, str) and len(str(v)) > 1
            )
            if valid_cols > max_valid_cols:
                max_valid_cols = valid_cols
                header_row = i
        
        return header_row
    
    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.loc[:, ~df.columns.astype(str).str.contains('Unnamed')]
        df = df.dropna(axis=1, how='all')
        return df

