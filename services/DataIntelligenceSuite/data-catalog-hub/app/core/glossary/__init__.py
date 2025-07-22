"""
Glossary management module for Data Catalog Hub

Combines basic glossary operations with AI-powered enhancements
for intelligent term mapping and discovery.
"""

from .manager import GlossaryManager
from .ai_enhancements import AIGlossaryEnhancements
from .models import BusinessTerm, TermMapping, TermStatus, TermCategory

__all__ = [
    'GlossaryManager',
    'AIGlossaryEnhancements',
    'BusinessTerm',
    'TermMapping',
    'TermStatus',
    'TermCategory'
] 