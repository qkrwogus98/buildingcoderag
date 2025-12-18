# graphrag_generator.py
"""
GraphRAG Generator - LLM 기반 응답 생성기
검색된 그래프 컨텍스트를 사용하여 자연어 응답 생성
"""

import logging
from typing import List, Dict, Optional
import os

logger = logging.getLogger(__name__)


class GraphRAGGenerator:
    """그래프 기반 RAG 응답 생성기"""

    def __init__(self, model: str = "gpt-4", api_key: Optional[str] = None):
        """
        초기화 함수 - OpenAI 클라이언트 설정
        
        Args:
            model: 사용할 LLM 모델명 (예: "gpt-4", "gpt-3.5-turbo")
            api_key: OpenAI API 키 (None이면 환경변수에서 가져옴)
        """
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")

        # OpenAI 클라이언트 초기화 시도
        try:
            # ✅ 신버전 OpenAI API 사용 (openai >= 1.0.0)
            from openai import OpenAI
            if self.api_key:
                self.client = OpenAI(api_key=self.api_key)
                self.use_openai = True
                logger.info(f"✅ GraphRAG Generator 초기화 (모델: {model})")
            else:
                logger.warning("⚠️  OpenAI API 키 없음 - 템플릿 기반 응답만 가능")
                self.use_openai = False
                self.client = None
        except ImportError:
            logger.warning("⚠️  OpenAI 패키지 없음 - 템플릿 기반 응답만 가능")
            self.use_openai = False
            self.client = None

    def generate_answer(self, query: str, context_articles: List[Dict],
                       max_tokens: int = 1000) -> str:
        """
        질문에 대한 답변 생성 함수
        
        1. 검색된 법령 조항들을 컨텍스트로 포맷팅
        2. LLM을 사용하여 자연스러운 답변 생성
        3. LLM이 없으면 템플릿 기반 답변 생성

        Args:
            query: 사용자 질문
            context_articles: 검색된 관련 조항들
            max_tokens: 최대 토큰 수

        Returns:
            생성된 답변
        """
        if not context_articles:
            return "관련된 법령 조항을 찾을 수 없습니다."

        # 컨텍스트 구성
        context = self._format_context(context_articles)

        if self.use_openai:
            return self._generate_with_llm(query, context, max_tokens)
        else:
            return self._generate_template_based(query, context_articles)

    def _format_context(self, articles: List[Dict]) -> str:
        """
        조항들을 컨텍스트 문자열로 포맷팅하는 함수
        예: "건축법 법률 제11조(건축허가)\n건축물을 건축하려는 자는..."

        Args:
            articles: 조항 리스트

        Returns:
            포맷팅된 컨텍스트 문자열
        """
        context_parts = []

        for i, article in enumerate(articles, 1):
            law_code = article.get('law_code', 'UNKNOWN')
            law_type = article.get('law_type', 'Act')
            article_id = article.get('article_id', '')
            title = article.get('title', '')
            text = article.get('text', '')

            # 법령 종류 한글화
            type_map = {'Act': '법률', 'Decree': '시행령', 'Rule': '시행규칙'}
            law_type_kr = type_map.get(law_type, law_type)

            context_parts.append(
                f"[조항 {i}] {law_code} {law_type_kr} {article_id}({title})\n{text}\n"
            )

        return "\n".join(context_parts)

    def _generate_with_llm(self, query: str, context: str, max_tokens: int) -> str:
        """
        LLM을 사용한 답변 생성 함수
        OpenAI의 GPT 모델을 사용하여 자연스러운 답변 생성

        Args:
            query: 사용자 질문
            context: 법령 조항 컨텍스트
            max_tokens: 최대 토큰 수

        Returns:
            생성된 답변
        """
        system_prompt = """당신은 한국 건축 관련 법령 전문가입니다.
주어진 법령 조항들을 바탕으로 사용자의 질문에 정확하고 상세하게 답변해주세요.

답변 시 다음 사항을 준수하세요:
1. 관련 조항 번호를 명시하여 답변하세요
2. 법률, 시행령, 시행규칙을 구분하여 설명하세요
3. 전문 용어는 쉽게 풀어서 설명하세요
4. 근거가 불충분하면 추가 확인이 필요하다고 안내하세요
"""

        user_prompt = f"""다음 법령 조항들을 참고하여 질문에 답변해주세요.

[관련 법령 조항]
{context}

[질문]
{query}

[답변]"""

        try:
            # ✅ 신버전 OpenAI API 사용 (openai >= 1.0.0)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=max_tokens,
                temperature=0.3
            )

            # 답변 추출
            answer = response.choices[0].message.content
            return answer

        except Exception as e:
            logger.error(f"LLM 생성 실패: {e}")
            return self._generate_template_based(query, [])

    def _generate_template_based(self, query: str, articles: List[Dict]) -> str:
        """
        템플릿 기반 답변 생성 (LLM 없이 사용 가능)
        OpenAI API 키가 없을 때 사용하는 기본 답변 방식
        
        간단히 검색된 조항들을 나열하는 방식

        Args:
            query: 사용자 질문
            articles: 조항 리스트

        Returns:
            생성된 답변
        """
        if not articles:
            return "관련된 법령 조항을 찾을 수 없습니다."

        answer_parts = [f"'{query}'에 대한 관련 법령 조항은 다음과 같습니다:\n"]

        for i, article in enumerate(articles, 1):
            law_code = article.get('law_code', 'UNKNOWN')
            law_type = article.get('law_type', 'Act')
            article_id = article.get('article_id', '')
            title = article.get('title', '')
            text = article.get('text', '')[:200]  # 처음 200자만

            type_map = {'Act': '법률', 'Decree': '시행령', 'Rule': '시행규칙'}
            law_type_kr = type_map.get(law_type, law_type)

            answer_parts.append(
                f"\n{i}. {law_code} {law_type_kr} {article_id}({title})\n{text}..."
            )

        answer_parts.append("\n\n더 자세한 내용은 해당 조항을 직접 확인해주세요.")

        return "\n".join(answer_parts)

    def generate_summary(self, articles: List[Dict]) -> str:
        """
        조항들의 요약 생성 함수
        여러 조항을 읽고 핵심 내용을 요약

        Args:
            articles: 조항 리스트

        Returns:
            요약문
        """
        if not articles:
            return "요약할 조항이 없습니다."

        context = self._format_context(articles)

        if self.use_openai:
            system_prompt = "당신은 한국 법령 요약 전문가입니다. 주어진 법령 조항들을 간결하게 요약해주세요."
            
            try:
                # ✅ 신버전 OpenAI API 사용
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"다음 법령 조항들을 요약해주세요:\n\n{context}"}
                    ],
                    max_tokens=500,
                    temperature=0.3
                )
                
                return response.choices[0].message.content
                
            except Exception as e:
                logger.error(f"요약 생성 실패: {e}")
                return f"주어진 {len(articles)}개 조항의 요약:\n{context[:300]}..."
        else:
            return f"주어진 {len(articles)}개 조항의 요약:\n{context[:300]}..."