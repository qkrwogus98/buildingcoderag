# graphrag_retriever.py
"""
GraphRAG Retriever - 그래프 기반 검색 시스템
Neo4j 그래프 데이터베이스에서 관련 법령 조항을 검색
"""

import logging
from typing import List, Dict, Tuple, Optional
from neo4j import GraphDatabase
import re

logger = logging.getLogger(__name__)


class GraphRAGRetriever:
    """그래프 기반 RAG 검색기"""

    def __init__(self, uri: str, user: str, password: str):
        """
        Args:
            uri: Neo4j URI
            user: Neo4j 사용자명
            password: Neo4j 비밀번호
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("✅ GraphRAG Retriever 초기화")

    def close(self):
        """연결 종료"""
        self.driver.close()

    def search_by_article_id(self, article_id: str, law_code: Optional[str] = None) -> List[Dict]:
        """
        조항 번호로 검색

        Args:
            article_id: 조항 번호 (예: "제1조", "제2조의2")
            law_code: 법령 코드 (선택, 예: "BUILDING")

        Returns:
            검색된 조항 리스트
        """
        with self.driver.session() as session:
            if law_code:
                query = """
                MATCH (a:Article {article_id: $article_id, law_code: $law_code})
                RETURN a.uid as uid, a.article_id as article_id, a.title as title,
                       a.law_code as law_code, a.law_type as law_type, a.full_text as text
                """
                result = session.run(query, {'article_id': article_id, 'law_code': law_code})
            else:
                query = """
                MATCH (a:Article {article_id: $article_id})
                RETURN a.uid as uid, a.article_id as article_id, a.title as title,
                       a.law_code as law_code, a.law_type as law_type, a.full_text as text
                """
                result = session.run(query, {'article_id': article_id})

            articles = []
            for record in result:
                articles.append({
                    'uid': record['uid'],
                    'article_id': record['article_id'],
                    'title': record['title'],
                    'law_code': record['law_code'],
                    'law_type': record['law_type'],
                    'text': record['text']
                })

            return articles

    def search_by_keyword(self, keyword: str, limit: int = 10) -> List[Dict]:
        """
        키워드로 조항 검색 (전문 검색)

        Args:
            keyword: 검색 키워드
            limit: 최대 결과 수

        Returns:
            검색된 조항 리스트
        """
        with self.driver.session() as session:
            query = """
            MATCH (a:Article)
            WHERE a.full_text CONTAINS $keyword OR a.title CONTAINS $keyword
            RETURN a.uid as uid, a.article_id as article_id, a.title as title,
                   a.law_code as law_code, a.law_type as law_type, a.full_text as text
            LIMIT $limit
            """
            result = session.run(query, {'keyword': keyword, 'limit': limit})

            articles = []
            for record in result:
                articles.append({
                    'uid': record['uid'],
                    'article_id': record['article_id'],
                    'title': record['title'],
                    'law_code': record['law_code'],
                    'law_type': record['law_type'],
                    'text': record['text']
                })

            return articles

    def get_related_articles(self, article_uid: str, relation_type: str = "REFERS_TO",
                           depth: int = 1) -> List[Dict]:
        """
        관련 조항 검색 (그래프 순회)

        Args:
            article_uid: 기준 조항 UID
            relation_type: 관계 타입 (REFERS_TO, DELEGATES_TO, CROSS_REFERS_TO, CONTAINS)
            depth: 검색 깊이

        Returns:
            관련 조항 리스트
        """
        with self.driver.session() as session:
            if relation_type == "ALL":
                query = f"""
                MATCH (a:Article {{uid: $uid}})-[r*1..{depth}]-(related:Article)
                RETURN DISTINCT related.uid as uid, related.article_id as article_id,
                       related.title as title, related.law_code as law_code,
                       related.law_type as law_type, related.full_text as text,
                       type(r[0]) as relation
                """
            else:
                query = f"""
                MATCH (a:Article {{uid: $uid}})-[r:{relation_type}*1..{depth}]-(related:Article)
                RETURN DISTINCT related.uid as uid, related.article_id as article_id,
                       related.title as title, related.law_code as law_code,
                       related.law_type as law_type, related.full_text as text,
                       '{relation_type}' as relation
                """

            result = session.run(query, {'uid': article_uid})

            articles = []
            for record in result:
                articles.append({
                    'uid': record['uid'],
                    'article_id': record['article_id'],
                    'title': record['title'],
                    'law_code': record['law_code'],
                    'law_type': record['law_type'],
                    'text': record['text'],
                    'relation': record.get('relation', relation_type)
                })

            return articles

    def get_article_with_context(self, article_id: str, law_code: str,
                                include_relations: bool = True) -> Dict:
        """
        조항과 그 문맥(관련 조항) 함께 검색

        Args:
            article_id: 조항 번호
            law_code: 법령 코드
            include_relations: 관련 조항 포함 여부

        Returns:
            조항 정보 + 관련 조항들
        """
        # 기본 조항 검색
        articles = self.search_by_article_id(article_id, law_code)

        if not articles:
            return None

        article = articles[0]
        result = {
            'main_article': article,
            'related_articles': []
        }

        if include_relations:
            # 관련 조항 검색
            refers_to = self.get_related_articles(article['uid'], 'REFERS_TO', depth=1)
            delegates_to = self.get_related_articles(article['uid'], 'DELEGATES_TO', depth=1)

            result['related_articles'] = {
                'refers_to': refers_to,
                'delegates_to': delegates_to
            }

        return result

    def get_article_hierarchy(self, article_uid: str) -> Dict:
        """
        조항의 계층 구조 (조 > 항 > 호 > 목) 가져오기

        Args:
            article_uid: 조항 UID

        Returns:
            계층 구조 정보
        """
        with self.driver.session() as session:
            # 조항 정보
            article_query = """
            MATCH (a:Article {uid: $uid})
            RETURN a.uid as uid, a.article_id as article_id, a.title as title,
                   a.law_code as law_code, a.law_type as law_type, a.full_text as text
            """
            article_result = session.run(article_query, {'uid': article_uid}).single()

            if not article_result:
                return None

            # 하위 항들
            clause_query = """
            MATCH (a:Article {uid: $uid})-[:CONTAINS]->(c:Clause)
            RETURN c.uid as uid, c.clause_id as clause_id, c.content as content
            ORDER BY c.clause_id
            """
            clause_result = session.run(clause_query, {'uid': article_uid})

            clauses = []
            for clause in clause_result:
                # 각 항의 호들
                item_query = """
                MATCH (c:Clause {uid: $uid})-[:CONTAINS]->(i:Item)
                RETURN i.uid as uid, i.item_id as item_id, i.content as content
                ORDER BY i.item_id
                """
                item_result = session.run(item_query, {'uid': clause['uid']})

                items = []
                for item in item_result:
                    # 각 호의 목들
                    subitem_query = """
                    MATCH (i:Item {uid: $uid})-[:CONTAINS]->(s:Subitem)
                    RETURN s.uid as uid, s.subitem_id as subitem_id, s.content as content
                    ORDER BY s.subitem_id
                    """
                    subitem_result = session.run(subitem_query, {'uid': item['uid']})

                    subitems = [{'uid': s['uid'], 'id': s['subitem_id'], 'content': s['content']}
                               for s in subitem_result]

                    items.append({
                        'uid': item['uid'],
                        'id': item['item_id'],
                        'content': item['content'],
                        'subitems': subitems
                    })

                clauses.append({
                    'uid': clause['uid'],
                    'id': clause['clause_id'],
                    'content': clause['content'],
                    'items': items
                })

            return {
                'article': {
                    'uid': article_result['uid'],
                    'article_id': article_result['article_id'],
                    'title': article_result['title'],
                    'law_code': article_result['law_code'],
                    'law_type': article_result['law_type'],
                    'text': article_result['text']
                },
                'clauses': clauses
            }

    def search_by_query(self, query: str, max_results: int = 5) -> List[Dict]:
        """
        자연어 쿼리로 검색 (키워드 추출 + 그래프 검색)

        Args:
            query: 자연어 질문
            max_results: 최대 결과 수

        Returns:
            검색된 조항 리스트 (관련도 순)
        """
        # 간단한 키워드 추출 (실제로는 더 정교한 방법 사용 가능)
        keywords = self._extract_keywords(query)

        all_articles = []
        seen_uids = set()

        # 각 키워드로 검색
        for keyword in keywords:
            articles = self.search_by_keyword(keyword, limit=max_results)

            for article in articles:
                if article['uid'] not in seen_uids:
                    all_articles.append(article)
                    seen_uids.add(article['uid'])

        # 상위 N개만 반환
        return all_articles[:max_results]

    def _extract_keywords(self, query: str) -> List[str]:
        """
        쿼리에서 키워드 추출 (간단한 버전)

        Args:
            query: 입력 쿼리

        Returns:
            추출된 키워드 리스트
        """
        # 불용어 제거 및 명사 추출 (간단한 버전)
        stopwords = ['은', '는', '이', '가', '을', '를', '에', '의', '와', '과', '도', '만',
                    '에서', '으로', '로', '에게', '한테', '께', '뭐', '어떻게', '무엇']

        # 공백으로 분리
        words = query.split()

        # 불용어 제거 및 길이 필터링
        keywords = [w for w in words if w not in stopwords and len(w) > 1]

        return keywords[:3]  # 상위 3개 키워드만 사용
