import io
import pandas as pd
from app.parsers.base import BaseParser


class CSVParser(BaseParser):
    ENCODINGS = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    @property
    def supported_extensions(self) -> list[str]:
        return ['.csv', '.tsv']
    
    @property
    def name(self) -> str:
        return "CSV"
    
    async def parse(
        self, 
        content: bytes, 
        filename: str,
        progress_callback: callable = None
    ) -> pd.DataFrame:

        if progress_callback:
            await progress_callback(35, "35% - Detecting encoding...")
        
        delimiter = '\t' if filename.lower().endswith('.tsv') else ','
        
        df = None
        last_error = None
        
        for idx, encoding in enumerate(self.ENCODINGS):
            try:
                df = pd.read_csv(
                    io.BytesIO(content), 
                    encoding=encoding,
                    delimiter=delimiter
                )
                
                if progress_callback:
                    progress = 40 + (idx * 3)
                    await progress_callback(progress, f"{progress}% - Parsing {self.name}...")
                break
                
            except UnicodeDecodeError as e:
                last_error = e
                continue
        
        if df is None:
            raise ValueError(
                f"Could not read {self.name} file. Please ensure it's a valid "
                f"{self.name} with UTF-8 or Latin encoding. Error: {last_error}"
            )
        
        return df
