# qa_dataset.py
"""
QA 데이터셋 로더 및 관리
건축 법령 관련 질의응답 데이터셋 처리
"""

import json
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class QACase:
    """QA 케이스 데이터 클래스"""
    case_id: str
    category: str
    title: str
    question: str
    answer: str
    reasoning: str
    date: str
    related_laws: List[Dict[str, str]]

    @classmethod
    def from_dict(cls, data: Dict) -> 'QACase':
        """딕셔너리에서 QACase 생성"""
        content = data.get('content', {})
        return cls(
            case_id=data.get('case_id', ''),
            category=data.get('category', ''),
            title=data.get('title', ''),
            question=content.get('question', ''),
            answer=content.get('answer', ''),
            reasoning=content.get('reasoning', ''),
            date=data.get('date', ''),
            related_laws=data.get('related_laws', [])
        )

    def to_dict(self) -> Dict:
        """딕셔너리로 변환"""
        return {
            'case_id': self.case_id,
            'category': self.category,
            'title': self.title,
            'content': {
                'question': self.question,
                'answer': self.answer,
                'reasoning': self.reasoning
            },
            'date': self.date,
            'related_laws': self.related_laws
        }


class QADataset:
    """QA 데이터셋 관리 클래스"""

    def __init__(self, json_path: str):
        """
        Args:
            json_path: QA 데이터셋 JSON 파일 경로
        """
        self.json_path = json_path
        self.cases: List[QACase] = []
        self._load()

    def _load(self):
        """JSON 파일에서 데이터 로드"""
        try:
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            cases_data = data.get('cases', [])
            self.cases = [QACase.from_dict(case) for case in cases_data]

            logger.info(f"✅ QA 데이터셋 로드 완료: {len(self.cases)}개 케이스")

        except FileNotFoundError:
            logger.error(f"QA 데이터셋 파일을 찾을 수 없음: {self.json_path}")
            self.cases = []
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 오류: {e}")
            self.cases = []
        except Exception as e:
            logger.error(f"데이터셋 로드 오류: {e}")
            self.cases = []

    def get_all_cases(self) -> List[QACase]:
        """모든 케이스 반환"""
        return self.cases

    def get_case_by_id(self, case_id: str) -> Optional[QACase]:
        """케이스 ID로 검색"""
        for case in self.cases:
            if case.case_id == case_id:
                return case
        return None

    def get_cases_by_category(self, category: str) -> List[QACase]:
        """카테고리별 케이스 검색"""
        return [case for case in self.cases if case.category == category]

    def get_categories(self) -> List[str]:
        """모든 카테고리 반환"""
        categories = set(case.category for case in self.cases)
        return sorted(list(categories))

    def get_cases_by_law(self, law_code: str, article_id: Optional[str] = None) -> List[QACase]:
        """
        특정 법령 또는 조항과 관련된 케이스 검색

        Args:
            law_code: 법령 코드 (예: "건축법")
            article_id: 조항 번호 (선택, 예: "제44조")

        Returns:
            관련 케이스 리스트
        """
        results = []

        for case in self.cases:
            for law in case.related_laws:
                if law.get('code') == law_code:
                    if article_id is None or article_id in law.get('article_id', ''):
                        results.append(case)
                        break

        return results

    def search(self, keyword: str) -> List[QACase]:
        """
        키워드로 케이스 검색 (질문, 답변, 제목에서 검색)

        Args:
            keyword: 검색 키워드

        Returns:
            검색 결과 케이스 리스트
        """
        results = []

        for case in self.cases:
            if (keyword in case.question or
                keyword in case.answer or
                keyword in case.title or
                keyword in case.reasoning):
                results.append(case)

        return results

    def get_statistics(self) -> Dict:
        """데이터셋 통계"""
        categories = self.get_categories()

        category_counts = {}
        for category in categories:
            category_counts[category] = len(self.get_cases_by_category(category))

        # 관련 법령 통계
        law_codes = {}
        for case in self.cases:
            for law in case.related_laws:
                code = law.get('code', 'Unknown')
                law_codes[code] = law_codes.get(code, 0) + 1

        return {
            'total_cases': len(self.cases),
            'total_categories': len(categories),
            'categories': category_counts,
            'law_codes': law_codes
        }

    def print_statistics(self):
        """통계 출력"""
        stats = self.get_statistics()

        print("\n" + "=" * 70)
        print("QA 데이터셋 통계")
        print("=" * 70)
        print(f"\n총 케이스 수: {stats['total_cases']}개")
        print(f"카테고리 수: {stats['total_categories']}개")

        print("\n[카테고리별 분포]")
        sorted_categories = sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True)
        for category, count in sorted_categories[:10]:  # 상위 10개만
            print(f"  {category[:50]:50s}: {count:3d}개")

        if len(sorted_categories) > 10:
            print(f"  ... 외 {len(sorted_categories) - 10}개 카테고리")

        print("\n[관련 법령 분포]")
        sorted_laws = sorted(stats['law_codes'].items(), key=lambda x: x[1], reverse=True)
        for law_code, count in sorted_laws:
            print(f"  {law_code:30s}: {count:3d}건")

        print("=" * 70)

    def export_to_jsonl(self, output_path: str):
        """JSONL 형식으로 내보내기 (LLM 학습용)"""
        with open(output_path, 'w', encoding='utf-8') as f:
            for case in self.cases:
                line = json.dumps(case.to_dict(), ensure_ascii=False)
                f.write(line + '\n')

        logger.info(f"✅ JSONL 내보내기 완료: {output_path}")

    def create_train_test_split(self, test_ratio: float = 0.2) -> tuple:
        """
        학습/테스트 데이터 분할

        Args:
            test_ratio: 테스트 데이터 비율

        Returns:
            (train_cases, test_cases) 튜플
        """
        import random

        cases_copy = self.cases.copy()
        random.shuffle(cases_copy)

        split_idx = int(len(cases_copy) * (1 - test_ratio))
        train_cases = cases_copy[:split_idx]
        test_cases = cases_copy[split_idx:]

        logger.info(f"데이터 분할: 학습 {len(train_cases)}개, 테스트 {len(test_cases)}개")

        return train_cases, test_cases


def main():
    """테스트용 메인 함수"""
    import os

    # 데이터셋 로드
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data.json')
    dataset = QADataset(data_path)

    # 통계 출력
    dataset.print_statistics()

    # 예제 케이스 출력
    print("\n[예제 케이스 1]")
    if dataset.cases:
        case = dataset.cases[0]
        print(f"ID: {case.case_id}")
        print(f"카테고리: {case.category}")
        print(f"제목: {case.title}")
        print(f"질문: {case.question}")
        print(f"답변: {case.answer}")
        print(f"관련 법령: {case.related_laws}")

    # 검색 테스트
    print("\n[검색 테스트: '건축허가']")
    results = dataset.search("건축허가")
    print(f"{len(results)}개 케이스 발견")
    for i, case in enumerate(results[:3], 1):
        print(f"  {i}. {case.case_id}: {case.title}")


if __name__ == "__main__":
    main()
