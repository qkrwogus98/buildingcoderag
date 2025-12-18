# graphrag_engine.py
"""
GraphRAG Engine - 그래프 기반 RAG 통합 엔진
Retriever와 Generator를 통합하여 질의응답 파이프라인 제공
"""

import logging
from typing import List, Dict, Optional, Tuple
from graphrag_retriever import GraphRAGRetriever
from graphrag_generator import GraphRAGGenerator

logger = logging.getLogger(__name__)


class GraphRAGEngine:
    """GraphRAG 통합 엔진"""

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str,
                 llm_model: str = "gpt-4", openai_api_key: Optional[str] = None):
        """
        Args:
            neo4j_uri: Neo4j URI
            neo4j_user: Neo4j 사용자명
            neo4j_password: Neo4j 비밀번호
            llm_model: LLM 모델명
            openai_api_key: OpenAI API 키
        """
        self.retriever = GraphRAGRetriever(neo4j_uri, neo4j_user, neo4j_password)
        self.generator = GraphRAGGenerator(llm_model, openai_api_key)
        logger.info("✅ GraphRAG Engine 초기화 완료")

    def close(self):
        """리소스 정리"""
        self.retriever.close()

    def query(self, question: str, max_results: int = 5,
             include_relations: bool = True) -> Dict:
        """
        자연어 질문에 답변

        Args:
            question: 사용자 질문
            max_results: 최대 검색 결과 수
            include_relations: 관련 조항 포함 여부

        Returns:
            {
                'question': 질문,
                'answer': 답변,
                'sources': 참조한 조항들,
                'metadata': 추가 정보
            }
        """
        logger.info(f"🔍 질문: {question}")

        # 1. 검색 단계
        articles = self.retriever.search_by_query(question, max_results=max_results)

        if not articles:
            return {
                'question': question,
                'answer': "관련된 법령 조항을 찾을 수 없습니다. 다른 키워드로 검색해보세요.",
                'sources': [],
                'metadata': {'found': 0}
            }

        logger.info(f"📚 {len(articles)}개 조항 검색됨")

        # 관련 조항 추가 검색
        if include_relations and articles:
            enriched_articles = []
            seen_uids = set()

            for article in articles:
                if article['uid'] not in seen_uids:
                    enriched_articles.append(article)
                    seen_uids.add(article['uid'])

                # 참조 조항 추가
                related = self.retriever.get_related_articles(
                    article['uid'], relation_type='REFERS_TO', depth=1
                )

                for rel_art in related[:2]:  # 각 조항당 최대 2개의 관련 조항
                    if rel_art['uid'] not in seen_uids:
                        enriched_articles.append(rel_art)
                        seen_uids.add(rel_art['uid'])

            articles = enriched_articles[:max_results * 2]  # 최대 결과의 2배까지

        # 2. 생성 단계
        answer = self.generator.generate_answer(question, articles)

        logger.info("✅ 답변 생성 완료")

        return {
            'question': question,
            'answer': answer,
            'sources': articles,
            'metadata': {
                'found': len(articles),
                'include_relations': include_relations
            }
        }

    def get_article_details(self, article_id: str, law_code: str) -> Dict:
        """
        특정 조항의 상세 정보 조회

        Args:
            article_id: 조항 번호 (예: "제1조")
            law_code: 법령 코드 (예: "BUILDING")

        Returns:
            조항 상세 정보
        """
        logger.info(f"🔍 조항 조회: {law_code} {article_id}")

        # 조항 + 관련 조항 검색
        article_data = self.retriever.get_article_with_context(
            article_id, law_code, include_relations=True
        )

        if not article_data:
            return {
                'found': False,
                'message': f"{law_code} {article_id}를 찾을 수 없습니다."
            }

        # 계층 구조 조회
        main_article = article_data['main_article']
        hierarchy = self.retriever.get_article_hierarchy(main_article['uid'])

        # 포맷팅
        formatted = self.generator.format_article_with_relations(article_data)

        return {
            'found': True,
            'article': main_article,
            'hierarchy': hierarchy,
            'related_articles': article_data.get('related_articles', {}),
            'formatted_text': formatted
        }

    def search_by_keyword(self, keyword: str, limit: int = 10) -> List[Dict]:
        """
        키워드로 조항 검색

        Args:
            keyword: 검색 키워드
            limit: 최대 결과 수

        Returns:
            검색 결과 리스트
        """
        logger.info(f"🔍 키워드 검색: {keyword}")

        articles = self.retriever.search_by_keyword(keyword, limit=limit)

        logger.info(f"📚 {len(articles)}개 조항 발견")

        return articles

    def summarize_articles(self, article_ids: List[Tuple[str, str]]) -> str:
        """
        여러 조항 요약

        Args:
            article_ids: [(article_id, law_code), ...] 형태의 리스트

        Returns:
            요약문
        """
        articles = []

        for article_id, law_code in article_ids:
            found = self.retriever.search_by_article_id(article_id, law_code)
            articles.extend(found)

        if not articles:
            return "요약할 조항을 찾을 수 없습니다."

        summary = self.generator.generate_summary(articles)

        return summary

    def compare_articles(self, article_id1: str, law_code1: str,
                        article_id2: str, law_code2: str) -> Dict:
        """
        두 조항 비교

        Args:
            article_id1: 첫 번째 조항 번호
            law_code1: 첫 번째 법령 코드
            article_id2: 두 번째 조항 번호
            law_code2: 두 번째 법령 코드

        Returns:
            비교 결과
        """
        article1_list = self.retriever.search_by_article_id(article_id1, law_code1)
        article2_list = self.retriever.search_by_article_id(article_id2, law_code2)

        if not article1_list or not article2_list:
            return {
                'found': False,
                'message': '조항을 찾을 수 없습니다.'
            }

        article1 = article1_list[0]
        article2 = article2_list[0]

        # LLM 사용 가능하면 비교 생성
        if self.generator.use_openai:
            comparison_prompt = f"""다음 두 법령 조항을 비교 분석해주세요:

[조항 1] {law_code1} {article_id1}({article1['title']})
{article1['text']}

[조항 2] {law_code2} {article_id2}({article2['title']})
{article2['text']}

공통점과 차이점을 중심으로 비교해주세요."""

            comparison = self.generator.generate_answer(
                comparison_prompt, [article1, article2]
            )
        else:
            comparison = f"""[조항 1] {law_code1} {article_id1}({article1['title']})
길이: {len(article1['text'])}자

[조항 2] {law_code2} {article_id2}({article2['title']})
길이: {len(article2['text'])}자

상세 비교는 LLM API 키가 필요합니다."""

        return {
            'found': True,
            'article1': article1,
            'article2': article2,
            'comparison': comparison
        }

    def find_related_chain(self, article_id: str, law_code: str,
                          relation_type: str = "REFERS_TO", depth: int = 2) -> Dict:
        """
        조항의 관계 체인 찾기 (그래프 순회)

        Args:
            article_id: 시작 조항 번호
            law_code: 법령 코드
            relation_type: 관계 타입
            depth: 탐색 깊이

        Returns:
            관계 체인 정보
        """
        articles = self.retriever.search_by_article_id(article_id, law_code)

        if not articles:
            return {
                'found': False,
                'message': f'{law_code} {article_id}를 찾을 수 없습니다.'
            }

        article = articles[0]

        # 관련 조항 체인 검색
        related = self.retriever.get_related_articles(
            article['uid'], relation_type=relation_type, depth=depth
        )

        # 요약 생성
        summary = f"{law_code} {article_id}와 {relation_type} 관계로 연결된 조항은 총 {len(related)}개입니다."

        return {
            'found': True,
            'start_article': article,
            'related_articles': related,
            'relation_type': relation_type,
            'depth': depth,
            'summary': summary
        }

    def interactive_query(self):
        """대화형 질의응답 모드"""
        print("=" * 70)
        print("GraphRAG 대화형 질의응답 시스템")
        print("=" * 70)
        print("명령어:")
        print("  - 일반 질문: 자유롭게 질문하세요")
        print("  - /article <법령코드> <조항번호>: 특정 조항 조회")
        print("  - /search <키워드>: 키워드 검색")
        print("  - /quit: 종료")
        print("=" * 70)

        while True:
            try:
                user_input = input("\n질문> ").strip()

                if not user_input:
                    continue

                if user_input == "/quit":
                    print("종료합니다.")
                    break

                if user_input.startswith("/article"):
                    parts = user_input.split()
                    if len(parts) >= 3:
                        law_code = parts[1]
                        article_id = parts[2]
                        result = self.get_article_details(article_id, law_code)
                        if result['found']:
                            print("\n" + result['formatted_text'])
                        else:
                            print(result['message'])
                    else:
                        print("사용법: /article <법령코드> <조항번호>")

                elif user_input.startswith("/search"):
                    keyword = user_input.replace("/search", "").strip()
                    if keyword:
                        articles = self.search_by_keyword(keyword, limit=5)
                        print(f"\n{len(articles)}개 조항 발견:")
                        for i, art in enumerate(articles, 1):
                            print(f"{i}. {art['law_code']} {art['article_id']}({art['title']})")
                    else:
                        print("사용법: /search <키워드>")

                else:
                    # 일반 질문
                    result = self.query(user_input, max_results=5)
                    print(f"\n답변:\n{result['answer']}")
                    print(f"\n참조 조항: {len(result['sources'])}개")

            except KeyboardInterrupt:
                print("\n\n종료합니다.")
                break
            except Exception as e:
                print(f"오류: {e}")
                logger.error(f"오류 발생: {e}", exc_info=True)
