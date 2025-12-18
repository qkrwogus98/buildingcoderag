# extractor.py
import os
import re
import pdfplumber
import logging

logger = logging.getLogger(__name__)

def extract_text_from_pdf(pdf_path: str, skip_toc=True) -> str:
    """PDF에서 텍스트 추출 (목차 제외)"""
    if not os.path.exists(pdf_path):
        logger.warning(f"파일 없음: {pdf_path}")
        return ""
    
    logger.info(f"📄 {os.path.basename(pdf_path)}")
    
    def is_toc_page(text: str) -> bool:
        if not text:
            return True
        lines = text.split('\n')
        article_lines = [l for l in lines if re.match(r'^\s*제\d+조', l)]
        has_clauses = bool(re.search(r'[①②③④⑤]', text))
        return len(article_lines) > 15 and not has_clauses
    
    text = ""
    skipped = 0
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if not page_text:
                    continue
                
                if skip_toc and is_toc_page(page_text):
                    skipped += 1
                    continue
                
                # 노이즈 제거
                lines = []
                for line in page_text.split('\n'):
                    line = line.strip()
                    if line in ["법제처", "국가법령정보센터"] or line.endswith("법"):
                        continue
                    if line.isdigit() or re.match(r'^법제처\s+\d+', line):
                        continue
                    lines.append(line)
                
                text += '\n'.join(lines) + "\n"
        
        logger.info(f"✅ {len(text):,}자 (목차 {skipped}p 제외)")
        return text
        
    except Exception as e:
        logger.error(f"PDF 추출 실패 {pdf_path}: {e}")
        return ""