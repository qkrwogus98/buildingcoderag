"""
GraphRAG QA 테스트 스크립트
data.json의 질문을 지식그래프에서 검색하여 정답과 비교
"""

import os
import sys
import json
import logging
from typing import Dict, List

# database 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'database'))

from database.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from database.graphrag_engine import GraphRAGEngine
from database.qa_dataset import QADataset, QACase

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_single_question(engine: GraphRAGEngine, case: QACase, max_results: int = 5) -> Dict:
    """
    단일 질문을 테스트하고 결과를 반환
    
    Args:
        engine: GraphRAG 엔진
        case: QA 케이스 (질문과 정답 포함)
        max_results: 최대 검색 결과 수
        
    Returns:
        테스트 결과 딕셔너리
    """
    logger.info(f"\n질문 ID: {case.case_id}")
    logger.info(f"카테고리: {case.category}")
    logger.info(f"제목: {case.title}")
    logger.info(f"질문: {case.question}")
    
    try:
        # GraphRAG로 질문에 대한 답변 생성
        result = engine.query(case.question, max_results=max_results)
        
        # 검색된 법령 조항 추출
        retrieved_laws = set()
        for source in result['sources']:
            law_code = source.get('law_code', '')
            article_id = source.get('article_id', '')
            if law_code and article_id:
                retrieved_laws.add((law_code, article_id))
        
        # 정답 법령 조항
        expected_laws = set()
        for law in case.related_laws:
            law_code = law.get('code', '')
            article_id = law.get('article_id', '')
            if law_code and article_id:
                expected_laws.add((law_code, article_id))
        
        # Recall 계산: 정답 중에서 검색된 비율
        if expected_laws:
            recall = len(expected_laws & retrieved_laws) / len(expected_laws)
        else:
            recall = 0.0
        
        # Precision 계산: 검색된 것 중에서 정답인 비율
        if retrieved_laws:
            precision = len(expected_laws & retrieved_laws) / len(retrieved_laws)
        else:
            precision = 0.0
        
        # F1 Score 계산
        if recall + precision > 0:
            f1_score = 2 * (recall * precision) / (recall + precision)
        else:
            f1_score = 0.0
        
        # 결과 출력
        logger.info(f"\n[생성된 답변]")
        logger.info(result['answer'][:300] + "..." if len(result['answer']) > 300 else result['answer'])
        
        logger.info(f"\n[정답]")
        logger.info(case.answer[:300] + "..." if len(case.answer) > 300 else case.answer)
        
        logger.info(f"\n[평가 지표]")
        logger.info(f"Recall (재현율):    {recall:.3f}")
        logger.info(f"Precision (정밀도): {precision:.3f}")
        logger.info(f"F1 Score:           {f1_score:.3f}")
        
        logger.info(f"\n[검색된 법령 조항]")
        for law_code, article_id in retrieved_laws:
            logger.info(f"  - {law_code} {article_id}")
        
        logger.info(f"\n[정답 법령 조항]")
        for law_code, article_id in expected_laws:
            logger.info(f"  - {law_code} {article_id}")
        
        return {
            'case_id': case.case_id,
            'question': case.question,
            'expected_answer': case.answer,
            'generated_answer': result['answer'],
            'expected_laws': list(expected_laws),
            'retrieved_laws': list(retrieved_laws),
            'recall': recall,
            'precision': precision,
            'f1_score': f1_score,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"오류 발생: {e}")
        return {
            'case_id': case.case_id,
            'question': case.question,
            'expected_answer': case.answer,
            'generated_answer': f"오류: {str(e)}",
            'expected_laws': [],
            'retrieved_laws': [],
            'recall': 0.0,
            'precision': 0.0,
            'f1_score': 0.0,
            'success': False
        }


def main():
    """메인 함수"""
    # data.json 경로 설정
    data_path = 'data.json'
    
    if not os.path.exists(data_path):
        logger.error(f"data.json 파일을 찾을 수 없습니다: {data_path}")
        sys.exit(1)
    
    # QA 데이터셋 로드
    logger.info(f"QA 데이터셋 로드: {data_path}")
    dataset = QADataset(data_path)
    
    if not dataset.cases:
        logger.error("데이터셋이 비어있습니다.")
        sys.exit(1)
    
    logger.info(f"총 {len(dataset.cases)}개의 질문이 로드되었습니다.\n")
    
    # GraphRAG 엔진 초기화
    logger.info("GraphRAG Engine 초기화...")
    try:
        engine = GraphRAGEngine(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
            llm_model="gpt-4",
            openai_api_key='api key here'  # OpenAI API 키 설정
        )
        logger.info("✓ GraphRAG Engine 초기화 성공\n")
    except Exception as e:
        logger.error(f"✗ Engine 초기화 실패: {e}")
        sys.exit(1)
    
    # 테스트 실행
    results = []
    
    # 예시: 처음 5개 질문만 테스트 (전체 테스트하려면 [:5] 제거)
    test_cases = dataset.cases[:5]
    
    logger.info("=" * 70)
    logger.info(f"총 {len(test_cases)}개의 질문을 테스트합니다.")
    logger.info("=" * 70)
    
    try:
        for i, case in enumerate(test_cases, 1):
            logger.info(f"\n{'='*70}")
            logger.info(f"[{i}/{len(test_cases)}] 테스트 진행 중...")
            logger.info(f"{'='*70}")
            
            result = test_single_question(engine, case)
            results.append(result)
        
        # 전체 결과 요약
        logger.info("\n" + "=" * 70)
        logger.info("전체 테스트 결과 요약")
        logger.info("=" * 70)
        
        successful = sum(1 for r in results if r['success'])
        avg_recall = sum(r['recall'] for r in results) / len(results)
        avg_precision = sum(r['precision'] for r in results) / len(results)
        avg_f1 = sum(r['f1_score'] for r in results) / len(results)
        
        logger.info(f"\n총 테스트: {len(results)}개")
        logger.info(f"성공: {successful}개 ({successful/len(results)*100:.1f}%)")
        logger.info(f"\n평균 Recall:    {avg_recall:.3f}")
        logger.info(f"평균 Precision: {avg_precision:.3f}")
        logger.info(f"평균 F1 Score:  {avg_f1:.3f}")
        
        # 결과 저장
        output_path = 'test_results.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"\n결과가 {output_path}에 저장되었습니다.")
        
    except KeyboardInterrupt:
        logger.info("\n\n테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 중 오류: {e}", exc_info=True)
    finally:
        engine.close()
        logger.info("\nGraphRAG Engine 종료")


if __name__ == "__main__":
    main()