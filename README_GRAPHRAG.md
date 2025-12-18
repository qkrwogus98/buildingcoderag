# GraphRAG for 건축 법령

건축 관련 법령을 위한 그래프 기반 RAG(Retrieval-Augmented Generation) 시스템

## 개요

이 시스템은 건축법, 건축물관리법 등 건축 관련 법령을 Neo4j 그래프 데이터베이스에 저장하고, 그래프 구조를 활용하여 효율적인 검색과 질의응답을 제공합니다.

## 빠른 시작

```bash
# 1. Neo4j 실행 (Docker)
docker run \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/DxI3O9BGnjGjdgu800HRd8kewNhHU9URb6lCMn3V4XI \
    -e NEO4J_PLUGINS='["apoc"]' \
    neo4j

# 2. 그래프 데이터베이스 구축 (별도 터미널)
python neo4j_build_graph.py

# 3. GraphRAG 시스템 테스트
python test_graphrag.py

# 4. 대화형 모드 실행
python graphrag_main.py --interactive

# 5. (선택) QA 데이터셋으로 평가
export OPENAI_API_KEY="your-api-key"
python evaluate_graphrag.py --limit 5 --detailed
```

## 주요 기능

### 1. 그래프 기반 검색
- **키워드 검색**: 법령 조항 내용에서 키워드로 검색
- **조항 번호 검색**: 특정 조항을 직접 조회
- **관계 기반 검색**: 조항 간 참조 관계를 따라 연관 조항 탐색

### 2. 질의응답 (RAG)
- **자연어 질문**: 일상 언어로 법령에 대해 질문
- **문맥 인식**: 관련 조항들을 종합하여 답변 생성
- **출처 제공**: 답변의 근거가 된 법령 조항 명시

### 3. 조항 분석
- **관계 체인 분석**: 조항 간 참조 관계 추적
- **계층 구조**: 조/항/호/목 계층 구조 제공
- **조항 비교**: 두 조항 간 비교 분석

## 시스템 구조

```
database/
├── config.py              # Neo4j 연결 설정
├── definitions.py         # 법령 정의
├── extractor.py          # PDF 텍스트 추출
├── parser.py             # 법령 파싱
├── graph_builder.py      # 그래프 구축
├── graphrag_retriever.py # 검색 엔진
├── graphrag_generator.py # 답변 생성기
└── graphrag_engine.py    # 통합 엔진

graphrag_main.py          # 메인 실행 파일
```

## 설치 및 설정

### 1. 필수 패키지 설치

```bash
pip install neo4j pdfplumber openai
```

### 2. Neo4j 데이터베이스 실행 (Docker)

Docker를 사용하여 Neo4j를 실행합니다:

```bash
docker run \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/DxI3O9BGnjGjdgu800HRd8kewNhHU9URb6lCMn3V4XI \
    -e NEO4J_PLUGINS='["apoc"]' \
    neo4j
```

접속 정보:
- Neo4j Browser: http://localhost:7474
- Bolt 연결: bolt://localhost:7687
- 사용자명: neo4j
- 비밀번호: DxI3O9BGnjGjdgu800HRd8kewNhHU9URb6lCMn3V4XI

### 3. 그래프 데이터베이스 구축

법령 PDF 파일을 그래프 데이터베이스로 구축:

```bash
# 루트 디렉토리에서 실행
python neo4j_build_graph.py
```

또는 database 모듈 사용:

```bash
cd database
python main.py
```

이 과정은 다음을 수행합니다:
- PDF에서 법령 조항 추출
- 조/항/호/목 계층 구조 파싱
- Neo4j 그래프 노드 및 관계 생성
- 조항 간 참조 관계 (REFERS_TO, DELEGATES_TO) 구축

### 4. QA 데이터셋

`data.json` 파일에는 59개의 건축 법령 관련 질의응답 케이스가 포함되어 있습니다.

데이터셋 구조:
```json
{
  "cases": [
    {
      "case_id": "12-0559",
      "category": "대지와 도로",
      "title": "건축물 대지와 도로 접의무 적용 예외",
      "content": {
        "question": "건축물의 대지가 반드시 건축법상 도로에 접하여야 하는지 여부",
        "answer": "건축법상 적용 제외 시 대지가 도로에 접하지 않아도 됨",
        "reasoning": "건축법 제44조 제1항 단서 및 예시 사유..."
      },
      "date": "2012.10.31.",
      "related_laws": [
        {
          "article_id": "제44조 제1항",
          "code": "건축법"
        }
      ]
    }
  ]
}
```

데이터셋 통계 확인:
```bash
python database/qa_dataset.py
```

## 사용법

### 1. 대화형 모드

```bash
python graphrag_main.py --interactive
```

대화형 모드에서 사용 가능한 명령어:
- 일반 질문: 자유롭게 입력
- `/article <법령코드> <조항번호>`: 특정 조항 조회
- `/search <키워드>`: 키워드 검색
- `/quit`: 종료

### 2. 키워드 검색

```bash
python graphrag_main.py --search "건축허가"
```

### 3. 특정 조항 조회

```bash
python graphrag_main.py --article BUILDING 제11조
```

법령 코드:
- `BUILDING`: 건축법
- `BUILDING_MGMT`: 건축물관리법

### 4. 자연어 질문

```bash
python graphrag_main.py --query "건축허가를 받아야 하는 경우는?"
```

### 5. 예제 실행

```bash
python graphrag_main.py --examples
```

### 6. OpenAI API 키 설정

LLM 기반 답변 생성을 위해 OpenAI API 키 필요:

```bash
# 환경변수 설정
export OPENAI_API_KEY="your-api-key"

# 또는 명령줄 옵션
python graphrag_main.py --api-key "your-api-key" --query "질문"
```

### 7. GraphRAG 시스템 평가

QA 데이터셋으로 시스템 성능 평가:

```bash
# 전체 데이터셋 평가
python evaluate_graphrag.py --data data.json --output results.json

# 일부만 평가 (예: 5개)
python evaluate_graphrag.py --limit 5

# 특정 카테고리만 평가
python evaluate_graphrag.py --category "대지와 도로"

# 상세 결과 출력
python evaluate_graphrag.py --limit 10 --detailed
```

평가 지표:
- **Recall (재현율)**: 정답 법령 중 검색된 비율
- **Precision (정밀도)**: 검색된 법령 중 정답인 비율
- **F1 Score**: Recall과 Precision의 조화평균

## Python 코드 사용 예제

```python
from database.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from database.graphrag_engine import GraphRAGEngine

# 엔진 초기화
engine = GraphRAGEngine(
    neo4j_uri=NEO4J_URI,
    neo4j_user=NEO4J_USER,
    neo4j_password=NEO4J_PASSWORD,
    llm_model="gpt-4",
    openai_api_key="your-api-key"
)

# 1. 자연어 질문
result = engine.query("건축허가는 어떻게 받나요?", max_results=5)
print(result['answer'])

# 2. 키워드 검색
articles = engine.search_by_keyword("건축허가", limit=10)
for article in articles:
    print(f"{article['article_id']}: {article['title']}")

# 3. 특정 조항 조회
details = engine.get_article_details("제11조", "BUILDING")
print(details['formatted_text'])

# 4. 관계 체인 분석
chain = engine.find_related_chain("제11조", "BUILDING", "REFERS_TO", depth=2)
print(f"관련 조항: {len(chain['related_articles'])}개")

# 종료
engine.close()
```

## 그래프 관계 유형

- **CONTAINS**: 계층 구조 (조 → 항 → 호 → 목)
- **REFERS_TO**: 같은 법령 내 조항 참조
- **DELEGATES_TO**: 위임 관계 (법 → 령 → 규칙)
- **CROSS_REFERS_TO**: 다른 법령 참조

## API 없이 사용

OpenAI API 키가 없어도 기본 기능 사용 가능:
- 키워드 검색
- 조항 조회
- 관계 탐색
- 템플릿 기반 답변

단, LLM 기반 자연어 답변 생성은 API 키가 필요합니다.

## 주요 클래스

### GraphRAGRetriever
그래프 기반 검색 기능 제공

```python
retriever = GraphRAGRetriever(uri, user, password)

# 키워드 검색
articles = retriever.search_by_keyword("건축허가")

# 조항 검색
articles = retriever.search_by_article_id("제11조", "BUILDING")

# 관련 조항
related = retriever.get_related_articles(article_uid, "REFERS_TO")
```

### GraphRAGGenerator
LLM 기반 답변 생성

```python
generator = GraphRAGGenerator(model="gpt-4", api_key="...")

# 답변 생성
answer = generator.generate_answer(query, context_articles)

# 요약 생성
summary = generator.generate_summary(articles)
```

### GraphRAGEngine
통합 엔진 (Retriever + Generator)

```python
engine = GraphRAGEngine(uri, user, password, model, api_key)

# 질의응답
result = engine.query("질문", max_results=5)

# 조항 상세
details = engine.get_article_details("제11조", "BUILDING")
```

## 개발 정보

- **Neo4j**: 그래프 데이터베이스
- **pdfplumber**: PDF 텍스트 추출
- **OpenAI API**: LLM 기반 답변 생성 (선택)
- **Python 3.7+** 필요

## 문제 해결

### Neo4j 연결 오류
- Neo4j 서버가 실행 중인지 확인
- `database/config.py`에서 연결 정보 확인

### 검색 결과 없음
- 그래프 데이터가 구축되었는지 확인
- 키워드를 다르게 시도

### LLM 답변 생성 실패
- OpenAI API 키 확인
- API 사용량 한도 확인
- 네트워크 연결 확인

## 라이선스

이 프로젝트는 법령 데이터를 활용한 연구 및 교육 목적으로 개발되었습니다.
