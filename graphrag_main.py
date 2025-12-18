#!/usr/bin/env python3
# graphrag_main.py
"""
GraphRAG 메인 실행 파일
건축 법령 GraphRAG 시스템 실행
"""

import os
import sys
import logging
import argparse

# database 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'database'))

from database.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from database.graphrag_engine import GraphRAGEngine


def setup_logger():
    """로거 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


logger = setup_logger()


def example_queries(engine: GraphRAGEngine):
    """예제 쿼리 실행"""
    print("\n" + "=" * 70)
    print("GraphRAG 예제 쿼리 실행")
    print("=" * 70)

    # 예제 1: 키워드 검색
    print("\n[예제 1] 키워드 검색: '건축허가'")
    print("-" * 70)
    articles = engine.search_by_keyword("건축허가", limit=3)
    for i, art in enumerate(articles, 1):
        print(f"{i}. {art['law_code']} {art['article_id']}({art['title']})")

    # 예제 2: 특정 조항 조회
    print("\n[예제 2] 특정 조항 조회: 건축법 제11조")
    print("-" * 70)
    result = engine.get_article_details("제11조", "BUILDING")
    if result['found']:
        print(result['formatted_text'][:500] + "...")

    # 예제 3: 자연어 질의응답
    print("\n[예제 3] 자연어 질문: '건축허가를 받아야 하는 경우는?'")
    print("-" * 70)
    qa_result = engine.query("건축허가를 받아야 하는 경우는?", max_results=3)
    print(f"답변:\n{qa_result['answer'][:500]}...")
    print(f"\n참조 조항: {len(qa_result['sources'])}개")

    # 예제 4: 관계 체인 검색
    print("\n[예제 4] 관계 체인 검색: 건축법 제11조의 참조 조항")
    print("-" * 70)
    chain_result = engine.find_related_chain("제11조", "BUILDING", "REFERS_TO", depth=2)
    if chain_result['found']:
        print(chain_result['summary'])
        print(f"관련 조항 {len(chain_result['related_articles'])}개:")
        for i, art in enumerate(chain_result['related_articles'][:5], 1):
            print(f"  {i}. {art['article_id']}({art['title']})")


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='건축 법령 GraphRAG 시스템',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예:
  # 대화형 모드
  python graphrag_main.py --interactive

  # 예제 쿼리 실행
  python graphrag_main.py --examples

  # 키워드 검색
  python graphrag_main.py --search "건축허가"

  # 특정 조항 조회
  python graphrag_main.py --article BUILDING 제11조

  # 질문
  python graphrag_main.py --query "건축허가를 받아야 하는 경우는?"
        """
    )

    parser.add_argument('--interactive', '-i', action='store_true',
                       help='대화형 모드로 실행')
    parser.add_argument('--examples', '-e', action='store_true',
                       help='예제 쿼리 실행')
    parser.add_argument('--search', '-s', type=str,
                       help='키워드로 검색')
    parser.add_argument('--article', '-a', nargs=2, metavar=('LAW_CODE', 'ARTICLE_ID'),
                       help='특정 조항 조회 (예: BUILDING 제11조)')
    parser.add_argument('--query', '-q', type=str,
                       help='자연어 질문')
    parser.add_argument('--model', '-m', type=str, default='gpt-4',
                       help='LLM 모델 (기본: gpt-4)')
    parser.add_argument('--api-key', '-k', type=str,
                       help='OpenAI API 키 (환경변수 OPENAI_API_KEY 사용 가능)')

    args = parser.parse_args()

    # API 키 설정
    api_key = args.api_key or os.getenv('OPENAI_API_KEY')

    # GraphRAG 엔진 초기화
    try:
        engine = GraphRAGEngine(
            neo4j_uri=NEO4J_URI,
            neo4j_user=NEO4J_USER,
            neo4j_password=NEO4J_PASSWORD,
            llm_model=args.model,
            openai_api_key=api_key
        )
    except Exception as e:
        logger.error(f"GraphRAG Engine 초기화 실패: {e}")
        sys.exit(1)

    try:
        # 대화형 모드
        if args.interactive:
            engine.interactive_query()

        # 예제 실행
        elif args.examples:
            example_queries(engine)

        # 키워드 검색
        elif args.search:
            print(f"\n키워드 검색: '{args.search}'")
            print("=" * 70)
            articles = engine.search_by_keyword(args.search, limit=10)
            if articles:
                for i, art in enumerate(articles, 1):
                    print(f"\n{i}. {art['law_code']} {art['law_type']} {art['article_id']}({art['title']})")
                    print(f"   {art['text'][:150]}...")
            else:
                print("검색 결과가 없습니다.")

        # 특정 조항 조회
        elif args.article:
            law_code, article_id = args.article
            print(f"\n조항 조회: {law_code} {article_id}")
            print("=" * 70)
            result = engine.get_article_details(article_id, law_code)
            if result['found']:
                print(result['formatted_text'])
            else:
                print(result['message'])

        # 자연어 질문
        elif args.query:
            print(f"\n질문: {args.query}")
            print("=" * 70)
            result = engine.query(args.query, max_results=5)
            print(f"\n답변:\n{result['answer']}")
            print("\n" + "=" * 70)
            print(f"참조 조항 ({len(result['sources'])}개):")
            for i, src in enumerate(result['sources'], 1):
                print(f"{i}. {src['law_code']} {src['article_id']}({src['title']})")

        # 아무 옵션도 없으면 help 출력
        else:
            parser.print_help()

    except KeyboardInterrupt:
        print("\n\n중단되었습니다.")
    except Exception as e:
        logger.error(f"실행 중 오류: {e}", exc_info=True)
    finally:
        engine.close()
        logger.info("GraphRAG Engine 종료")


if __name__ == "__main__":
    main()
