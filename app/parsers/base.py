from abc import ABC, abstractmethod
from typing import AsyncGenerator
import pandas as pd


class BaseParser(ABC):
    
    @property
    @abstractmethod
    def supported_extensions(self) -> list[str]:
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @abstractmethod
    async def parse(
        self, 
        content: bytes, 
        filename: str,
        progress_callback: callable = None
    ) -> pd.DataFrame:
        pass
    
    def can_parse(self, filename: str) -> bool:
        lower_filename = filename.lower()
        return any(lower_filename.endswith(ext) for ext in self.supported_extensions)
