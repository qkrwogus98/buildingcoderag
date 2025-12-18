"""
PDF 파일을 5쪽씩 나눠서 저장하는 스크립트
"""

from pypdf import PdfReader, PdfWriter
import os


def split_pdf_by_pages(input_pdf_path, pages_per_file=5, output_folder="output_pdfs"):
    """
    PDF 파일을 지정된 페이지 수만큼 나눠서 여러 개의 PDF 파일로 저장합니다.
    
    Parameters:
    -----------
    input_pdf_path : str
        나눌 원본 PDF 파일의 경로
    pages_per_file : int
        각 파일에 포함될 페이지 수 (기본값: 5)
    output_folder : str
        분할된 PDF 파일들을 저장할 폴더 경로 (기본값: "output_pdfs")
    
    Returns:
    --------
    list : 생성된 PDF 파일들의 경로 리스트
    """
    
    # PDF 파일 읽기
    print(f"PDF 파일 읽는 중: {input_pdf_path}")
    reader = PdfReader(input_pdf_path)
    total_pages = len(reader.pages)
    print(f"총 페이지 수: {total_pages}")
    
    # 출력 폴더가 없으면 생성
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"출력 폴더 생성: {output_folder}")
    
    # 원본 파일명 (확장자 제외)
    base_filename = os.path.splitext(os.path.basename(input_pdf_path))[0]
    
    # 생성된 파일 경로를 저장할 리스트
    created_files = []
    
    # 페이지를 나누어 저장
    file_number = 1
    page_index = 0
    
    while page_index < total_pages:
        # 새로운 PdfWriter 객체 생성 (각 분할 파일용)
        writer = PdfWriter()
        
        # 현재 파일에 추가할 페이지의 시작과 끝 인덱스 계산
        start_page = page_index
        end_page = min(page_index + pages_per_file, total_pages)
        
        # 지정된 범위의 페이지들을 writer에 추가
        for i in range(start_page, end_page):
            writer.add_page(reader.pages[i])
        
        # 출력 파일명 생성 (예: original_part1.pdf, original_part2.pdf)
        output_filename = f"{base_filename}_part{file_number}.pdf"
        output_path = os.path.join(output_folder, output_filename)
        
        # PDF 파일로 저장
        with open(output_path, "wb") as output_file:
            writer.write(output_file)
        
        print(f"생성됨: {output_filename} (페이지 {start_page + 1}-{end_page})")
        created_files.append(output_path)
        
        # 다음 파일을 위해 인덱스 업데이트
        page_index = end_page
        file_number += 1
    
    print(f"\n완료! 총 {len(created_files)}개의 파일이 생성되었습니다.")
    return created_files


# 사용 예시
if __name__ == "__main__":
    # 여기에 나누고 싶은 PDF 파일 경로를 입력하세요
    input_pdf = "raw_data.pdf"  # 실제 파일명으로 변경하세요
    
    # PDF를 5쪽씩 나누기
    try:
        split_files = split_pdf_by_pages(
            input_pdf_path=input_pdf,
            pages_per_file=5,  # 원하는 페이지 수로 변경 가능
            output_folder="pdf_data"  # 원하는 폴더명으로 변경 가능
        )
        
        print("\n생성된 파일 목록:")
        for file_path in split_files:
            print(f"  - {file_path}")
            
    except FileNotFoundError:
        print(f"오류: '{input_pdf}' 파일을 찾을 수 없습니다.")
        print("파일 경로를 확인해주세요.")
    except Exception as e:
        print(f"오류 발생: {e}")