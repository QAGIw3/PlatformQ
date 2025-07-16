"""Core functionality for the Redoc document conversion framework."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .converters import get_converter
from .exceptions import RedocError
from .ocr import OCRProcessor

logger = logging.getLogger(__name__)


class Redoc:
    """Main class for document conversion operations.
    
    This class provides a high-level interface for converting between different
    document formats, performing OCR, and generating documents using AI.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Redoc converter.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.ocr_processor = OCRProcessor()
        
    def convert(
        self,
        source: Union[str, Path, Dict],
        target_format: str,
        output_file: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> Any:
        """Convert a document from one format to another.
        
        Args:
            source: Source file path or template dictionary
            target_format: Target format (pdf, html, xml, json, docx, epub)
            output_file: Optional output file path
            **kwargs: Additional conversion options
            
        Returns:
            Converted content or path to output file
            
        Raises:
            RedocError: If conversion fails
        """
        try:
            converter = get_converter(target_format.lower())
            return converter.convert(source, output_file=output_file, **kwargs)
        except Exception as e:
            logger.error(f"Conversion failed: {str(e)}")
            raise RedocError(f"Failed to convert document: {str(e)}") from e
    
    def ocr(
        self,
        source: Union[str, Path],
        output_file: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Perform OCR on a document or image.
        
        Args:
            source: Path to the input file
            output_file: Optional output file path
            **kwargs: Additional OCR options
            
        Returns:
            Dictionary containing OCR results
        """
        return self.ocr_processor.process(source, output_file=output_file, **kwargs)
    
    def generate(
        self,
        prompt: str,
        format: str = 'pdf',
        output_file: Optional[Union[str, Path]] = None,
        **kwargs
    ) -> Any:
        """Generate a document using AI.
        
        Args:
            prompt: Text prompt for document generation
            format: Output format (default: pdf)
            output_file: Optional output file path
            **kwargs: Additional generation options
            
        Returns:
            Generated document content or path to output file
        """
        try:
            # TODO: Implement AI-powered document generation
            raise NotImplementedError("AI generation is not yet implemented")
        except Exception as e:
            logger.error(f"Document generation failed: {str(e)}")
            raise RedocError(f"Failed to generate document: {str(e)}") from e
