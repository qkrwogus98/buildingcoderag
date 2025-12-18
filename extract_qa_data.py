import os
import json
import requests
from openai import OpenAI
from bs4 import BeautifulSoup
import pdfplumber
from typing import List, Dict
import time

# 1. 설정
UPSTAGE_API_KEY = "api_key_here"  # Upstage API 키 설정
target_file = "raw_data.pdf"
output_filename = "qa_data_full.json"
debug_folder = "debug_texts"

# 디버그 폴더 생성
os.makedirs(debug_folder, exist_ok=True)

# 전역 변수로 전체 케이스 저장
all_cases = []


def get_total_pages(pdf_path: str) -> int:
    """
    PDF의 전체 페이지 수를 반환하는 함수
    
    Args:
        pdf_path: PDF 파일 경로
    
    Returns:
        전체 페이지 수
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception as e:
        print(f"페이지 수 확인 오류: {e}")
        return 0


def extract_pages_with_overlap(pdf_path: str, start_page: int, end_page: int, 
                                overlap_pages: int = 1) -> str:
    """
    PDF에서 특정 페이지 범위를 추출하되, 이전 청크의 마지막 페이지를 포함하는 함수
    (질문-답변이 페이지를 걸쳐있을 때를 대비)
    
    Args:
        pdf_path: PDF 파일 경로
        start_page: 시작 페이지 (0부터 시작)
        end_page: 끝 페이지 (포함)
        overlap_pages: 이전 청크와 겹치는 페이지 수 (기본 1페이지)
    
    Returns:
        추출된 텍스트
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 오버랩을 고려한 실제 시작 페이지
            # 첫 번째 청크가 아니면 overlap_pages만큼 앞에서 시작
            actual_start = max(0, start_page - overlap_pages) if start_page > 0 else start_page
            
            text = ""
            for page_num in range(actual_start, min(end_page + 1, len(pdf.pages))):
                page = pdf.pages[page_num]
                page_text = page.extract_text()
                
                if page_text:
                    # 페이지 구분자 추가 (디버깅용)
                    text += f"\n--- 페이지 {page_num + 1} ---\n"
                    text += page_text + "\n"
            
            return text
    except Exception as e:
        print(f"페이지 추출 오류: {e}")
        return ""


def parse_pdf_chunk_with_upstage(file_path: str, start_page: int, end_page: int) -> str:
    """
    Upstage API를 사용하여 PDF의 특정 페이지 범위를 파싱하는 함수
    주의: Upstage API는 전체 파일을 받으므로, 이 함수는 전체 파일을 업로드하고
    클라이언트 측에서 페이지를 필터링합니다.
    
    Args:
        file_path: PDF 파일 경로
        start_page: 시작 페이지 번호
        end_page: 끝 페이지 번호
    
    Returns:
        추출된 텍스트
    """
    # 실제로는 pdfplumber로 페이지별 추출이 더 효율적
    # Upstage API는 전체 문서를 한번에 처리하므로, 청크별로는 pdfplumber 사용
    return extract_pages_with_overlap(file_path, start_page, end_page)


def extract_data_with_solar(text_content: str, chunk_num: int) -> str:
    """
    Solar Pro LLM을 사용하여 텍스트에서 구조화된 데이터를 추출하는 함수
    
    Args:
        text_content: 추출할 텍스트 내용
        chunk_num: 현재 청크 번호 (디버깅용)
    
    Returns:
        JSON 형식의 문자열
    """
    client = OpenAI(
        api_key=UPSTAGE_API_KEY,
        base_url="https://api.upstage.ai/v1"
    )

    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "construction_law_case",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "cases": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "case_id": {"type": "string"},
                                "date": {"type": "string"},
                                "category": {"type": "string"},
                                "title": {"type": "string"},
                                "content": {
                                    "type": "object",
                                    "properties": {
                                        "question": {"type": "string"},
                                        "answer": {"type": "string"},
                                        "reasoning": {"type": "string"}
                                    },
                                    "required": ["question", "answer", "reasoning"],
                                    "additionalProperties": False
                                },
                                "related_laws": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "code": {"type": "string"},
                                            "article_id": {"type": "string"}
                                        },
                                        "required": ["code", "article_id"],
                                        "additionalProperties": False
                                    }
                                }
                            },
                            "required": ["case_id", "title", "content", "related_laws"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["cases"],
                "additionalProperties": False
            }
        }
    }

    # 텍스트 길이 제한 (Solar Pro의 컨텍스트 윈도우 고려)
    input_text = text_content[:25000]
    
    messages = [
        {
            "role": "system",
            "content": """You are a legal data extraction assistant specialized in Korean construction law documents.

CRITICAL INSTRUCTIONS for data extraction:
1. Extract case_id, date, category, and title as metadata
2. For "question" field: Extract the 질의 (question) section
3. For "answer" field: Extract the 회신 (response/answer) section
4. For "reasoning" field: **COPY THE EXACT TEXT from the 이유(사유) section WITHOUT ANY MODIFICATION, SUMMARIZATION, OR PARAPHRASING**
   - Include ALL the original text from the reasoning section
   - Preserve the exact wording, punctuation, and structure
   - Do NOT summarize or shorten the reasoning
   - Do NOT paraphrase or rewrite the reasoning
   - This is the most important field - accuracy is critical
5. Extract related_laws with proper law codes and article numbers

IMPORTANT: 
- If a case spans across page breaks (indicated by "--- 페이지 X ---"), treat it as a single continuous case
- Complete cases that start in this chunk but may be cut off at the end
- Skip incomplete cases that are clearly cut off at the beginning (these will be captured in the next chunk with overlap)

The reasoning section typically appears after the answer and explains the legal basis for the decision."""
        },
        {
            "role": "user",
            "content": f"""Extract all complete legal cases from the following Korean construction law document.

REMEMBER: 
1. For the "reasoning" field, you MUST copy the exact original text without any changes.
2. If a case is split across pages, combine them into one complete case.
3. Only extract complete cases - skip cases that are clearly cut off at the start.

Text to analyze (Chunk #{chunk_num}):

{input_text}"""
        }
    ]

    try:
        response = client.chat.completions.create(
            model="solar-pro2",
            messages=messages,
            response_format=response_format,
            temperature=0.1  # 일관성을 위해 낮은 temperature
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"LLM 추출 오류 (청크 {chunk_num}): {e}")
        return json.dumps({"cases": []})


def deduplicate_cases(cases: List[Dict]) -> List[Dict]:
    """
    중복된 케이스를 제거하는 함수
    case_id와 title을 기준으로 중복 판단
    
    Args:
        cases: 케이스 리스트
    
    Returns:
        중복이 제거된 케이스 리스트
    """
    seen = set()
    unique_cases = []
    
    for case in cases:
        # case_id가 있으면 case_id로, 없으면 title로 중복 체크
        identifier = case.get('case_id', '') or case.get('title', '')
        
        if identifier and identifier not in seen:
            seen.add(identifier)
            unique_cases.append(case)
        elif not identifier:
            # identifier가 없으면 일단 포함 (추후 수동 확인 필요)
            unique_cases.append(case)
    
    return unique_cases


def process_pdf_in_chunks(pdf_path: str, chunk_size: int = 5, overlap: int = 1):
    """
    PDF를 청크 단위로 나눠서 처리하는 메인 함수
    
    Args:
        pdf_path: PDF 파일 경로
        chunk_size: 한 번에 처리할 페이지 수 (기본 5페이지)
        overlap: 청크 간 겹치는 페이지 수 (기본 1페이지)
    """
    print("=" * 80)
    print(f"PDF 처리 시작: {pdf_path}")
    print("=" * 80)
    
    # 전체 페이지 수 확인
    total_pages = get_total_pages(pdf_path)
    if total_pages == 0:
        print("오류: PDF 파일을 열 수 없습니다.")
        return
    
    print(f"전체 페이지 수: {total_pages}")
    print(f"청크 크기: {chunk_size}페이지")
    print(f"오버랩: {overlap}페이지")
    print(f"예상 청크 수: {(total_pages + chunk_size - 1) // chunk_size}")
    print("=" * 80)
    
    # 청크별로 처리
    chunk_num = 0
    for start_page in range(0, total_pages, chunk_size):
        chunk_num += 1
        end_page = min(start_page + chunk_size - 1, total_pages - 1)
        
        print(f"\n{'='*80}")
        print(f"청크 {chunk_num} 처리 중: 페이지 {start_page + 1} ~ {end_page + 1}")
        print(f"{'='*80}")
        
        # 1. 페이지 추출 (오버랩 포함)
        print(f"  [1/4] 페이지 추출 중...")
        text_content = extract_pages_with_overlap(pdf_path, start_page, end_page, overlap)
        
        if not text_content or len(text_content) < 100:
            print(f"  ⚠️  경고: 추출된 텍스트가 너무 짧습니다 ({len(text_content)}자). 스킵합니다.")
            continue
        
        print(f"  ✅ 추출 완료: {len(text_content):,}자")
        
        # 디버그: 추출된 텍스트 저장
        debug_file = os.path.join(debug_folder, f"chunk_{chunk_num:03d}_pages_{start_page+1}-{end_page+1}.txt")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(text_content)
        print(f"  📝 디버그 파일 저장: {debug_file}")
        
        # 2. LLM으로 데이터 추출
        print(f"  [2/4] LLM 데이터 추출 중...")
        json_result = extract_data_with_solar(text_content, chunk_num)
        
        # 3. JSON 파싱
        print(f"  [3/4] JSON 파싱 중...")
        try:
            chunk_data = json.loads(json_result)
            chunk_cases = chunk_data.get("cases", [])
            print(f"  ✅ 추출된 케이스 수: {len(chunk_cases)}개")
            
            # 4. 전체 리스트에 추가
            all_cases.extend(chunk_cases)
            
        except json.JSONDecodeError as e:
            print(f"  ❌ JSON 파싱 오류: {e}")
            continue
        
        # API 속도 제한을 위한 대기 (필요시)
        if chunk_num < (total_pages + chunk_size - 1) // chunk_size:
            print(f"  [4/4] 다음 청크를 위해 2초 대기...")
            time.sleep(2)
    
    print(f"\n{'='*80}")
    print(f"모든 청크 처리 완료!")
    print(f"{'='*80}")
    print(f"총 추출된 케이스 수 (중복 포함): {len(all_cases)}개")
    
    # 중복 제거
    print(f"\n중복 케이스 제거 중...")
    unique_cases = deduplicate_cases(all_cases)
    print(f"✅ 중복 제거 완료: {len(all_cases)}개 -> {len(unique_cases)}개")
    
    # 최종 결과 저장
    print(f"\n최종 결과 저장 중: {output_filename}")
    final_data = {"cases": unique_cases}
    
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ 저장 완료!")
    
    # 통계 출력
    print(f"\n{'='*80}")
    print(f"📊 최종 통계")
    print(f"{'='*80}")
    print(f"  전체 페이지: {total_pages}페이지")
    print(f"  처리된 청크: {chunk_num}개")
    print(f"  추출된 케이스: {len(unique_cases)}개")
    print(f"  제거된 중복: {len(all_cases) - len(unique_cases)}개")
    
    # 샘플 케이스 출력
    if len(unique_cases) > 0:
        print(f"\n{'='*80}")
        print(f"📄 첫 번째 케이스 미리보기")
        print(f"{'='*80}")
        first_case = unique_cases[0]
        print(f"  Case ID: {first_case.get('case_id', 'N/A')}")
        print(f"  Date: {first_case.get('date', 'N/A')}")
        print(f"  Category: {first_case.get('category', 'N/A')}")
        print(f"  Title: {first_case.get('title', 'N/A')[:80]}...")
        print(f"  Question 길이: {len(first_case['content']['question'])}자")
        print(f"  Answer 길이: {len(first_case['content']['answer'])}자")
        print(f"  Reasoning 길이: {len(first_case['content']['reasoning'])}자")
        print(f"  Related Laws: {len(first_case.get('related_laws', []))}개")


# 메인 실행
if __name__ == "__main__":
    try:
        process_pdf_in_chunks(
            pdf_path=target_file,
            chunk_size=3,  # 3페이지씩 처리
            overlap=1      # 1페이지 오버랩
        )
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()