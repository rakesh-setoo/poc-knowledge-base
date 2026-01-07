from typing import Optional
from app.parsers.base import BaseParser
from app.parsers.csv_parser import CSVParser
from app.parsers.excel_parser import ExcelParser


class ParserRegistry:
    _parsers: list[BaseParser] = []
    
    @classmethod
    def register(cls, parser: BaseParser) -> None:
        cls._parsers.append(parser)
    
    @classmethod
    def get_parser(cls, filename: str) -> Optional[BaseParser]:
        for parser in cls._parsers:
            if parser.can_parse(filename):
                return parser
        return None
    
    @classmethod
    def get_supported_extensions(cls) -> list[str]:
        extensions = []
        for parser in cls._parsers:
            extensions.extend(parser.supported_extensions)
        return extensions
    
    @classmethod
    def get_supported_extensions_display(cls) -> str:
        return ", ".join(cls.get_supported_extensions())


# Register default parsers
ParserRegistry.register(CSVParser())
ParserRegistry.register(ExcelParser())


# Convenience function for direct import
def get_parser(filename: str) -> Optional[BaseParser]:
    return ParserRegistry.get_parser(filename)
