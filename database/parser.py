# parser.py
import re
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class LawParser:
    """법령 파서"""
    
    def parse(self, text: str, law_code: str, law_type: str) -> Dict:
        articles_data = []
        pattern = re.compile(r'\n(제\d+조(?:의\d+)?)\(([^)]+)\)')
        matches = list(pattern.finditer(text))
        
        for i, match in enumerate(matches):
            article_id = match.group(1)
            title = match.group(2)
            start = match.start()
            end = matches[i+1].start() if i+1 < len(matches) else len(text)
            article_text = text[start:end].strip()
            
            clauses = self._parse_clauses(article_text)
            if clauses or len(article_text) > 100:
                articles_data.append({
                    'id': article_id,
                    'title': title,
                    'text': article_text,
                    'clauses': clauses
                })
        
        unique = {}
        for art in articles_data:
            aid = art['id']
            if aid not in unique or len(art['text']) > len(unique[aid]['text']):
                unique[aid] = art
        
        result = list(unique.values())
        logger.info(f"✅ {law_code}-{law_type}: {len(result)}개 조항")
        
        return {
            'law_code': law_code,
            'law_type': law_type,
            'articles': result
        }
    
    def _parse_clauses(self, text: str) -> List[Dict]:
        clauses = []
        parts = re.split(r'([①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳])', text)
        i = 0
        while i < len(parts):
            part = parts[i]
            if re.match(r'[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]', part):
                if i + 1 < len(parts):
                    clause_text = parts[i+1].strip()
                    if clause_text:
                        items = self._parse_items(clause_text)
                        clauses.append({'id': part, 'text': clause_text, 'items': items})
                i += 2
            else:
                i += 1
        return clauses
    
    def _parse_items(self, text: str) -> List[Dict]:
        items = []
        lines = text.split('\n')
        current_item = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line: continue
            match = re.match(r'^(\d+)\.\s', line)
            if match:
                if current_item and current_lines:
                    item_text = '\n'.join(current_lines)
                    items.append({'id': current_item, 'text': item_text, 'subitems': self._parse_subitems(item_text)})
                current_item = match.group(1)
                current_lines = [line]
            elif current_item:
                current_lines.append(line)
        
        if current_item and current_lines:
            item_text = '\n'.join(current_lines)
            items.append({'id': current_item, 'text': item_text, 'subitems': self._parse_subitems(item_text)})
        return items
    
    def _parse_subitems(self, text: str) -> List[Dict]:
        subitems = []
        lines = text.split('\n')
        current_sub = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line: continue
            match = re.match(r'^([가-힣])\.\s', line)
            if match:
                if current_sub and current_lines:
                    subitems.append({'id': current_sub, 'text': '\n'.join(current_lines)})
                current_sub = match.group(1)
                current_lines = [line]
            elif current_sub:
                current_lines.append(line)
        
        if current_sub and current_lines:
            subitems.append({'id': current_sub, 'text': '\n'.join(current_lines)})
        return subitems