#!/usr/bin/env python3
# evaluate_graphrag.py
"""
GraphRAG 시스템 평가 스크립트
QA 데이터셋을 사용하여 GraphRAG 시스템의 성능 평가
"""

import os
import sys
import json
import logging
import argparse
from typing import List, Dict
from datetime import datetime

# database 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'database'))

from database.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from database.graphrag_engine import GraphRAGEngine
from database.qa_dataset import QADataset, QACase

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GraphRAGEvaluator:
    """GraphRAG 시스템 평가기"""

    def __init__(self, engine: GraphRAGEngine, dataset: QADataset):
        """
        Args:
            engine: GraphRAG 엔진
            dataset: QA 데이터셋
        """
        self.engine = engine
        self.dataset = dataset
        self.results = []

    def evaluate_case(self, case: QACase, max_results: int = 5) -> Dict:
        """
        개별 케이스 평가

        Args:
            case: QA 케이스
            max_results: 최대 검색 결과 수

        Returns:
            평가 결과
        """
        logger.info(f"평가 중: {case.case_id} - {case.title}")

        # GraphRAG로 질문에 답변 생성
        try:
            result = self.engine.query(case.question, max_results=max_results)

            # 관련 법령이 검색되었는지 확인
            retrieved_laws = set()
            for src in result['sources']:
                law_code = src.get('law_code', '')
                article_id = src.get('article_id', '')
                if law_code and article_id:
                    retrieved_laws.add((law_code, article_id))

            # 정답 법령
            expected_laws = set()
            for law in case.related_laws:
                law_code = law.get('code', '')
                article_id = law.get('article_id', '')
                if law_code and article_id:
                    # 법령 코드 매핑 (건축법 -> BUILDING)
                    code_mapping = {
                        '건축법': 'BUILDING',
                        '건축법 시행령': 'BUILDING',
                        '건축법 시행규칙': 'BUILDING',
                        '건축물관리법': 'BUILDING_MGMT',
                        '건축물관리법 시행령': 'BUILDING_MGMT',
                        '건축물관리법 시행규칙': 'BUILDING_MGMT'
                    }
                    mapped_code = code_mapping.get(law_code, law_code)

                    # 조항 번호 정규화 (제44조 제1항 -> 제44조)
                    article_base = article_id.split()[0] if ' ' in article_id else article_id
                    expected_laws.add((mapped_code, article_base))

            # 재현율 및 정밀도 계산 (법령 기준)
            if expected_laws:
                # 검색된 법령 중 정답에 있는 것
                true_positives = 0
                for law_code, article_id in retrieved_laws:
                    article_base = article_id.split()[0] if ' ' in article_id else article_id
                    if (law_code, article_base) in expected_laws:
                        true_positives += 1

                recall = true_positives / len(expected_laws) if expected_laws else 0
                precision = true_positives / len(retrieved_laws) if retrieved_laws else 0
                f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            else:
                recall = 0
                precision = 0
                f1 = 0

            eval_result = {
                'case_id': case.case_id,
                'category': case.category,
                'title': case.title,
                'question': case.question,
                'expected_answer': case.answer,
                'expected_laws': list(expected_laws),
                'generated_answer': result['answer'],
                'retrieved_sources': len(result['sources']),
                'retrieved_laws': list(retrieved_laws),
                'recall': recall,
                'precision': precision,
                'f1_score': f1,
                'success': True
            }

        except Exception as e:
            logger.error(f"평가 실패: {case.case_id} - {e}")
            eval_result = {
                'case_id': case.case_id,
                'category': case.category,
                'title': case.title,
                'question': case.question,
                'expected_answer': case.answer,
                'expected_laws': [],
                'generated_answer': f"오류: {str(e)}",
                'retrieved_sources': 0,
                'retrieved_laws': [],
                'recall': 0,
                'precision': 0,
                'f1_score': 0,
                'success': False
            }

        return eval_result

    def evaluate_all(self, limit: Optional[int] = None,
                    category: Optional[str] = None) -> List[Dict]:
        """
        전체 데이터셋 평가

        Args:
            limit: 평가할 케이스 수 제한 (None이면 전체)
            category: 특정 카테고리만 평가 (None이면 전체)

        Returns:
            평가 결과 리스트
        """
        cases = self.dataset.get_all_cases()

        # 카테고리 필터링
        if category:
            cases = [c for c in cases if c.category == category]

        # 개수 제한
        if limit:
            cases = cases[:limit]

        logger.info(f"총 {len(cases)}개 케이스 평가 시작...")

        self.results = []
        for i, case in enumerate(cases, 1):
            print(f"\n[{i}/{len(cases)}] 평가 중...")
            result = self.evaluate_case(case)
            self.results.append(result)

        return self.results

    def print_summary(self):
        """평가 결과 요약 출력"""
        if not self.results:
            print("평가 결과가 없습니다.")
            return

        total = len(self.results)
        successful = sum(1 for r in self.results if r['success'])

        # 평균 지표 계산
        avg_recall = sum(r['recall'] for r in self.results) / total
        avg_precision = sum(r['precision'] for r in self.results) / total
        avg_f1 = sum(r['f1_score'] for r in self.results) / total

        print("\n" + "=" * 70)
        print("GraphRAG 평가 결과 요약")
        print("=" * 70)
        print(f"\n총 평가 케이스: {total}개")
        print(f"성공: {successful}개 ({successful/total*100:.1f}%)")
        print(f"실패: {total - successful}개")

        print(f"\n[평균 성능 지표]")
        print(f"Recall (재현율):    {avg_recall:.3f}")
        print(f"Precision (정밀도): {avg_precision:.3f}")
        print(f"F1 Score:           {avg_f1:.3f}")

        # 카테고리별 성능
        category_stats = {}
        for result in self.results:
            cat = result['category']
            if cat not in category_stats:
                category_stats[cat] = {'count': 0, 'f1_sum': 0}
            category_stats[cat]['count'] += 1
            category_stats[cat]['f1_sum'] += result['f1_score']

        print(f"\n[카테고리별 F1 Score]")
        sorted_cats = sorted(category_stats.items(),
                           key=lambda x: x[1]['f1_sum']/x[1]['count'],
                           reverse=True)

        for cat, stats in sorted_cats[:10]:  # 상위 10개만
            avg_f1 = stats['f1_sum'] / stats['count']
            print(f"  {cat[:50]:50s}: {avg_f1:.3f} ({stats['count']}건)")

        print("=" * 70)

    def save_results(self, output_path: str):
        """평가 결과 저장"""
        output_data = {
            'timestamp': datetime.now().isoformat(),
            'total_cases': len(self.results),
            'results': self.results
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ 평가 결과 저장: {output_path}")

    def print_detailed_results(self, top_n: int = 5):
        """상세 결과 출력 (상위 N개)"""
        print("\n" + "=" * 70)
        print(f"상위 {top_n}개 케이스 상세 결과")
        print("=" * 70)

        # F1 스코어 기준 정렬
        sorted_results = sorted(self.results, key=lambda x: x['f1_score'], reverse=True)

        for i, result in enumerate(sorted_results[:top_n], 1):
            print(f"\n[{i}] {result['case_id']}: {result['title']}")
            print(f"카테고리: {result['category']}")
            print(f"질문: {result['question']}")
            print(f"\n정답: {result['expected_answer']}")
            print(f"\n생성된 답변: {result['generated_answer'][:200]}...")
            print(f"\n성능: Recall={result['recall']:.2f}, Precision={result['precision']:.2f}, F1={result['f1_score']:.2f}")
            print(f"검색된 조항: {result['retrieved_sources']}개")
            print("-" * 70)


from typing import Optional


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='GraphRAG 시스템 평가')
    parser.add_argument('--data', '-d', type=str, default='data.json',
                       help='QA 데이터셋 경로 (기본: data.json)')
    parser.add_argument('--limit', '-l', type=int,
                       help='평가할 케이스 수 제한')
    parser.add_argument('--category', '-c', type=str,
                       help='특정 카테고리만 평가')
    parser.add_argument('--output', '-o', type=str, default='evaluation_results.json',
                       help='결과 저장 경로 (기본: evaluation_results.json)')
    parser.add_argument('--model', '-m', type=str, default='gpt-4',
                       help='LLM 모델 (기본: gpt-4)')
    parser.add_argument('--api-key', '-k', type=str,
                       help='OpenAI API 키')
    parser.add_argument('--detailed', action='store_true',
                       help='상세 결과 출력')

    args = parser.parse_args()

    # API 키 설정
    api_key = 'api key here'  # 기본 API 키 설정

    # QA 데이터셋 로드
    logger.info(f"QA 데이터셋 로드: {args.data}")
    dataset = QADataset(args.data)

    if not dataset.cases:
        logger.error("데이터셋이 비어있습니다.")
        sys.exit(1)

    # GraphRAG 엔진 초기화
    logger.info("GraphRAG Engine 초기화...")
    try:
        engine = GraphRAGEngine(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
            llm_model=args.model,
            openai_api_key=api_key
        )
    except Exception as e:
        logger.error(f"Engine 초기화 실패: {e}")
        sys.exit(1)

    # 평가기 생성
    evaluator = GraphRAGEvaluator(engine, dataset)

    try:
        # 평가 실행
        evaluator.evaluate_all(limit=args.limit, category=args.category)

        # 결과 출력
        evaluator.print_summary()

        if args.detailed:
            evaluator.print_detailed_results(top_n=5)

        # 결과 저장
        evaluator.save_results(args.output)

    except KeyboardInterrupt:
        print("\n\n평가가 중단되었습니다.")
    except Exception as e:
        logger.error(f"평가 중 오류: {e}", exc_info=True)
    finally:
        engine.close()


if __name__ == "__main__":
    main()
