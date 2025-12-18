# main.py
import os
import datetime
from log_utils import setup_custom_logger
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from definitions import LAWS
from extractor import extract_text_from_pdf
from parser import LawParser
from graph_builder import GraphBuilder

# 로거 초기화 (가장 먼저 실행)
logger = setup_custom_logger()

def main():
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n\n{'#'*70}")
    logger.info(f"🚀 실행 시작: {now_str}")
    logger.info(f"{'#'*70}\n")
    
    # 처리할 법령 코드
    laws_to_process = ['BUILDING', 'BUILDING_MGMT']
    
    parser = LawParser()
    builder = GraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # builder.clear()  # 필요시 주석 해제
        builder.create_indexes()
        
        for law_code in laws_to_process:
            if law_code not in LAWS:
                logger.warning(f"정의되지 않은 법령 코드: {law_code}")
                continue
                
            law_def = LAWS[law_code]
            logger.info(f"\n{'='*70}\n{law_def.name} 처리\n{'='*70}")
            
            for law_type, pdf_path in law_def.pdf_paths.items():
                if not os.path.exists(pdf_path):
                    logger.warning(f"⚠️  파일 없음: {pdf_path}")
                    continue
                
                text = extract_text_from_pdf(pdf_path, skip_toc=True)
                if text:
                    parsed = parser.parse(text, law_code, law_type)
                    builder.build(parsed, law_def)
        
        logger.info(f"\n{'='*70}")
        builder.create_relations()
        builder.stats()
        
        logger.info("✅ 모든 작업 완료!")
        
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}", exc_info=True)
    finally:
        builder.close()

if __name__ == "__main__":
    main()