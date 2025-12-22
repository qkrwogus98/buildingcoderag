"""
건축 관련 법령 Graph RAG - Upstage Document Parse API 활용 버전
=============================================================

기존 pdfplumber 대신 Upstage Document Parse API를 사용하여
더 정확한 PDF 텍스트 추출과 구조 파싱을 수행합니다.

지원 법령: 건축법, 건축물관리법, 주택법, 국토계획법, 주차장법, 건축서비스산업진흥법 등
"""

import os
import re
import json
import logging
import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional
from collections import defaultdict

from neo4j import GraphDatabase

# Upstage 관련 라이브러리
# try:
#     from langchain_upstage import UpstageDocumentParseLoader
#     UPSTAGE_LANGCHAIN_AVAILABLE = True
# except ImportError:
UPSTAGE_LANGCHAIN_AVAILABLE = False
print("⚠️  langchain_upstage 패키지가 없습니다. pip install langchain-upstage로 설치하세요.")

import requests  # 직접 API 호출용 백업


# ==========================================
# 로거 설정
# ==========================================
def setup_custom_logger():
    """
    콘솔과 파일 모두에 로그를 출력하는 로거 설정
    
    Returns:
        logger: 설정된 로거 객체
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 기존 핸들러 제거 (중복 출력 방지)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    
    # 콘솔 출력 핸들러
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # 파일 저장 핸들러
    file_handler = logging.FileHandler('build_graph_log.txt', mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_custom_logger()


# ==========================================
# 법령 정의 (Dataclass)
# ==========================================
@dataclass
class LawDefinition:
    """
    법령 정의 클래스
    
    Attributes:
        code: 법령 코드 (예: BUILDING, HOUSING)
        name: 법령명 (예: 건축법, 주택법)
        act_label: Neo4j 레이블 - 법률
        decree_label: Neo4j 레이블 - 시행령
        rule_label: Neo4j 레이블 - 시행규칙
        pdf_paths: PDF 파일 경로 딕셔너리
    """
    code: str
    name: str
    act_label: str
    decree_label: str
    rule_label: str
    pdf_paths: Dict[str, str]


# 지원하는 법령 목록
LAWS = {
    'BUILDING': LawDefinition(
        code='BUILDING',
        name='건축법',
        act_label='BuildingAct',
        decree_label='BuildingDecree',
        rule_label='BuildingRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/건축법(법률)(제21065호)(20251001).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/건축법 시행령(대통령령)(제35811호)(20251001).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/건축법 시행규칙(국토교통부령)(제01531호)(20251031).pdf'
        }
    ),
    'BUILDING_MGMT': LawDefinition(
        code='BUILDING_MGMT',
        name='건축물관리법',
        act_label='BuildingMgmtAct',
        decree_label='BuildingMgmtDecree',
        rule_label='BuildingMgmtRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/건축물관리법(법률)(제20549호)(20250604).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/건축물관리법 시행령(대통령령)(제35549호)(20250604).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/건축물관리법 시행규칙(국토교통부령)(제01495호)(20250602).pdf'
        }
    ),
    'BUILDING_SERVICE': LawDefinition(
        code='BUILDING_SERVICE',
        name='건축서비스산업진흥법',
        act_label='BuildingServiceAct',
        decree_label='BuildingServiceDecree',
        rule_label='BuildingServiceRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/건축서비스산업 진흥법(법률)(제19990호)(20240710).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/건축서비스산업 진흥법 시행령(대통령령)(제33466호)(20230516).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/건축서비스산업 진흥법 시행규칙(국토교통부령)(제00098호)(20140605).pdf'
        }
    ),
    'PARKING': LawDefinition(
        code='PARKING',
        name='주차장법',
        act_label='ParkingAct',
        decree_label='ParkingDecree',
        rule_label='ParkingRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/주차장법(법률)(제21185호)(20251202).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/주차장법 시행령(대통령령)(제35708호)(20250817).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/주차장법 시행규칙(국토교통부령)(제01527호)(20250930).pdf'
        }
    ),
    'LAND_PLAN': LawDefinition(
        code='LAND_PLAN',
        name='국토의계획및이용에관한법률',
        act_label='LandPlanAct',
        decree_label='LandPlanDecree',
        rule_label='LandPlanRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/국토의 계획 및 이용에 관한 법률(법률)(제21065호)(20251001).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/국토의 계획 및 이용에 관한 법률 시행령(대통령령)(제35628호)(20251002).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/국토의 계획 및 이용에 관한 법률 시행규칙(국토교통부령)(제01338호)(20241130).pdf'
        }
    ),
    'HOUSING': LawDefinition(
        code='HOUSING',
        name='주택법',
        act_label='HousingAct',
        decree_label='HousingDecree',
        rule_label='HousingRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/주택법.pdf',
            'Decree': '/home/jaehyeonpark/Downloads/주택법 시행령.pdf',
            'Rule': '/home/jaehyeonpark/Downloads/주택법 시행규칙.pdf'
        }
    ),
    'GREEN_BUILDING': LawDefinition(
        code='GREEN_BUILDING',
        name='녹색건축물조성지원법',
        act_label='GreenBuildingAct',
        decree_label='GreenBuildingDecree',
        rule_label='GreenBuildingRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/녹색건축물 조성 지원법(법률)(제21065호)(20251001).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/녹색건축물 조성 지원법 시행령(대통령령)(제35811호)(20251001).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/녹색건축물 조성 지원법 시행규칙(국토교통부령)(제01422호)(20250101).pdf'
        }
    ),
    'HANOK': LawDefinition(
        code='HANOK',
        name='한옥등건축자산의진흥에관한법률',
        act_label='HanokAct',
        decree_label='HanokDecree',
        rule_label='HanokRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률(법률)(제19702호)(20240915).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률 시행령(대통령령)(제34494호)(20240517).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률 시행규칙(국토교통부령)(제00882호)(20210827).pdf'
        }
    ),
    'BUILDING_SALE': LawDefinition(
        code='BUILDING_SALE',
        name='건축물의분양에관한법률',
        act_label='BuildingSaleAct',
        decree_label='BuildingSaleDecree',
        rule_label='BuildingSaleRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률(법률)(제19702호)(20240915).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률 시행령(대통령령)(제34494호)(20240517).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/한옥 등 건축자산의 진흥에 관한 법률 시행규칙(국토교통부령)(제00882호)(20210827).pdf'
        }
    ),
    'CONVENIENCE': LawDefinition(
        code='CONVENIENCE',
        name='장애인노인임산부등의편의증진보장에관한법률',
        act_label='ConvenienceAct',
        decree_label='ConvenienceDecree',
        rule_label='ConvenienceRule',
        pdf_paths={
            'Act': '/home/jaehyeonpark/Downloads/장애인ㆍ노인ㆍ임산부 등의 편의증진 보장에 관한 법률(법률)(제20594호)(20251221).pdf',
            'Decree': '/home/jaehyeonpark/Downloads/장애인ㆍ노인ㆍ임산부 등의 편의증진 보장에 관한 법률 시행령(대통령령)(제35811호)(20251001).pdf',
            'Rule': '/home/jaehyeonpark/Downloads/장애인ㆍ노인ㆍ임산부 등의 편의증진 보장에 관한 법률 시행규칙(보건복지부령)(제01139호)(20251221).pdf'
        }
    ),
}


# ==========================================
# Upstage API를 사용한 PDF 추출기
# ==========================================
class UpstageDocumentExtractor:
    """
    Upstage Document Parse API를 사용하여 PDF에서 텍스트를 추출하는 클래스
    
    langchain_upstage 패키지를 우선 사용하고,
    없으면 requests로 직접 API 호출합니다.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Upstage 추출기 초기화
        
        Args:
            api_key: Upstage API 키. 없으면 환경변수에서 가져옴
        """
        # API 키 설정 (환경변수 우선)
        self.api_key = api_key or os.getenv("UPSTAGE_API_KEY")
        if not self.api_key:
            raise ValueError("UPSTAGE_API_KEY 환경변수를 설정하거나 api_key를 전달해주세요.")
        
        # API 엔드포인트
        self.api_url = "https://api.upstage.ai/v1/document-ai/document-parse"
        
        logger.info("✅ Upstage Document Extractor 초기화 완료")
    
    def extract_with_langchain(self, pdf_path: str) -> str:
        """
        langchain_upstage를 사용하여 PDF 텍스트 추출
        
        Args:
            pdf_path: PDF 파일 경로
            
        Returns:
            추출된 텍스트 전체
        """
        if not UPSTAGE_LANGCHAIN_AVAILABLE:
            raise ImportError("langchain_upstage 패키지가 필요합니다.")
        
        logger.info(f"📄 [LangChain] {os.path.basename(pdf_path)} 파싱 중...")
        
        # UpstageDocumentParseLoader 사용
        loader = UpstageDocumentParseLoader(
            file_path=pdf_path,
            split="page",           # 페이지별 분리
            output_format="html",   # HTML 형식 출력
            ocr="auto"              # PDF는 텍스트 우선, 이미지는 OCR
        )
        
        # 문서 로드 (lazy_load로 메모리 효율 개선)
        docs = []
        for doc in loader.lazy_load():
            docs.append(doc)
        
        # 모든 페이지 텍스트 합치기
        full_text = "\n\n".join([doc.page_content for doc in docs])
        
        # 노이즈 제거 (법제처, 국가법령정보센터 등)
        full_text = self._clean_text(full_text)
        
        logger.info(f"✅ 추출 완료: {len(full_text):,}자 ({len(docs)}페이지)")
        return full_text
    
    def extract_with_api(self, pdf_path: str) -> str:
        """
        Upstage API를 직접 호출하여 PDF 텍스트 추출 (백업 메서드)
        
        Args:
            pdf_path: PDF 파일 경로
            
        Returns:
            추출된 텍스트 전체
        """
        logger.info(f"📄 [API 직접호출] {os.path.basename(pdf_path)} 파싱 중...")
        
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # 파일 업로드
        with open(pdf_path, "rb") as f:
            files = {
                "document": (os.path.basename(pdf_path), f, "application/pdf")
            }
            data = {
                "output_formats": '["text"]',  # 텍스트 형식 출력
                "ocr": "auto"
            }
            
            response = requests.post(
                self.api_url,
                headers=headers,
                files=files,
                data=data
            )
        
        if response.status_code != 200:
            logger.error(f"❌ API 오류: {response.status_code} - {response.text}")
            return ""
        
        result = response.json()
        
        # 텍스트 추출
        full_text = result.get("content", {}).get("text", "")
        
        # 노이즈 제거
        full_text = self._clean_text(full_text)
        
        logger.info(f"✅ 추출 완료: {len(full_text):,}자")
        return full_text
    
    def extract(self, pdf_path: str) -> str:
        """
        PDF에서 텍스트 추출 (자동으로 최적의 방법 선택)
        
        Args:
            pdf_path: PDF 파일 경로
            
        Returns:
            추출된 텍스트
        """
        if not os.path.exists(pdf_path):
            logger.warning(f"⚠️  파일 없음: {pdf_path}")
            return ""
        
        # langchain_upstage 우선 사용
        if UPSTAGE_LANGCHAIN_AVAILABLE:
            try:
                return self.extract_with_langchain(pdf_path)
            except Exception as e:
                logger.warning(f"⚠️  LangChain 실패, API 직접 호출로 전환: {e}")
        
        # 백업: 직접 API 호출
        try:
            return self.extract_with_api(pdf_path)
        except Exception as e:
            logger.error(f"❌ PDF 추출 실패: {e}")
            return ""
    
    def _clean_text(self, text: str) -> str:
        """
        텍스트에서 노이즈 제거
        
        Args:
            text: 원본 텍스트
            
        Returns:
            정리된 텍스트
        """
        lines = []
        for line in text.split('\n'):
            line = line.strip()
            
            # 노이즈 패턴 제거
            if line in ["법제처", "국가법령정보센터"]:
                continue
            if line.endswith("법") and len(line) <= 10:
                continue
            if line.isdigit():
                continue
            if re.match(r'^법제처\s+\d+', line):
                continue
            if re.match(r'^\d+\s*/\s*\d+$', line):  # 페이지 번호 (예: 1 / 100)
                continue
                
            lines.append(line)
        
        return '\n'.join(lines)


# ==========================================
# 법령 파서
# ==========================================
class LawParser:
    """
    추출된 법령 텍스트를 파싱하여 구조화된 데이터로 변환하는 클래스
    
    조(Article) > 항(Clause) > 호(Item) > 목(Subitem) 구조로 파싱
    """
    
    def parse(self, text: str, law_code: str, law_type: str) -> Dict:
        """
        법령 텍스트를 파싱하여 구조화
        
        Args:
            text: PDF에서 추출한 텍스트
            law_code: 법령 코드 (예: BUILDING)
            law_type: 법령 종류 (Act, Decree, Rule)
            
        Returns:
            파싱된 데이터 딕셔너리
        """
        articles_data = []
        
        # 조항 패턴: "제N조(제목)" 또는 "제N조의N(제목)"
        pattern = re.compile(r'\n(제\d+조(?:의\d+)?)\(([^)]+)\)')
        matches = list(pattern.finditer(text))
        
        for i, match in enumerate(matches):
            article_id = match.group(1)      # 예: 제1조, 제2조의2
            title = match.group(2)            # 예: 목적, 정의
            
            # 조항 텍스트 범위 결정
            start = match.start()
            end = matches[i+1].start() if i+1 < len(matches) else len(text)
            article_text = text[start:end].strip()
            
            # 항(Clause) 파싱
            clauses = self._parse_clauses(article_text)
            # 유효한 조항만 추가 (최소 내용 있거나 항이 있는 경우)
            if clauses or len(article_text) > 20:
                articles_data.append({
                    'id': article_id,
                    'title': title,
                    'text': article_text,
                    'clauses': clauses
                })
        
        # 중복 제거 (동일 조항 ID가 여러 번 나오면 긴 텍스트 우선)
        unique = {}
        for art in articles_data:
            aid = art['id']
            if aid not in unique or len(art['text']) > len(unique[aid]['text']):
                unique[aid] = art
        
        result = list(unique.values())
        logger.info(f"✅ 파싱 완료 - {law_code}/{law_type}: {len(result)}개 조항")
        
        return {
            'law_code': law_code,
            'law_type': law_type,
            'articles': result
        }
    
    def _parse_clauses(self, text: str) -> List[Dict]:
        """
        조항 내에서 항(①②③...) 파싱
        
        Args:
            text: 조항 텍스트
            
        Returns:
            항 목록
        """
        clauses = []
        # 원문자 숫자로 항 분리
        clause_markers = '①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳'
        parts = re.split(f'([{clause_markers}])', text)
        
        i = 0
        while i < len(parts):
            part = parts[i]
            if part in clause_markers:
                if i + 1 < len(parts):
                    clause_text = parts[i+1].strip()
                    if clause_text:
                        # 호(Item) 파싱
                        items = self._parse_items(clause_text)
                        clauses.append({
                            'id': part,
                            'text': clause_text,
                            'items': items
                        })
                i += 2
            else:
                i += 1
        
        return clauses
    
    def _parse_items(self, text: str) -> List[Dict]:
        """
        항 내에서 호(1. 2. 3. ...) 파싱
        
        Args:
            text: 항 텍스트
            
        Returns:
            호 목록
        """
        items = []
        lines = text.split('\n')
        current_item = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 호 패턴: "N. " (숫자 + 마침표 + 공백)
            match = re.match(r'^(\d+)\.\s', line)
            if match:
                # 이전 호 저장
                if current_item and current_lines:
                    item_text = '\n'.join(current_lines)
                    subitems = self._parse_subitems(item_text)
                    items.append({
                        'id': current_item,
                        'text': item_text,
                        'subitems': subitems
                    })
                
                current_item = match.group(1)
                current_lines = [line]
            elif current_item:
                current_lines.append(line)
        
        # 마지막 호 저장
        if current_item and current_lines:
            item_text = '\n'.join(current_lines)
            subitems = self._parse_subitems(item_text)
            items.append({
                'id': current_item,
                'text': item_text,
                'subitems': subitems
            })
        
        return items
    
    def _parse_subitems(self, text: str) -> List[Dict]:
        """
        호 내에서 목(가. 나. 다. ...) 파싱
        
        Args:
            text: 호 텍스트
            
        Returns:
            목 목록
        """
        subitems = []
        lines = text.split('\n')
        current_sub = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # 목 패턴: "가. " (한글 자음 + 마침표 + 공백)
            match = re.match(r'^([가-힣])\.\s', line)
            if match:
                if current_sub and current_lines:
                    subitems.append({
                        'id': current_sub,
                        'text': '\n'.join(current_lines)
                    })
                
                current_sub = match.group(1)
                current_lines = [line]
            elif current_sub:
                current_lines.append(line)
        
        if current_sub and current_lines:
            subitems.append({
                'id': current_sub,
                'text': '\n'.join(current_lines)
            })
        
        return subitems


# ==========================================
# Neo4j 그래프 빌더
# ==========================================
class GraphBuilder:
    """
    파싱된 법령 데이터를 Neo4j 그래프 데이터베이스에 저장하는 클래스
    
    노드 종류:
    - Article: 조항
    - Clause: 항
    - Item: 호
    - Subitem: 목
    
    관계 종류:
    - CONTAINS: 포함 관계 (조항→항, 항→호, 호→목)
    - REFERS_TO: 동일 법령 내 참조
    - DELEGATES_TO: 위임 관계 (법→령, 령→규칙)
    - CROSS_REFERS_TO: 다른 법령 간 참조
    """
    
    def __init__(self, uri: str, user: str, password: str):
        """
        Neo4j 연결 초기화
        
        Args:
            uri: Neo4j 서버 URI
            user: 사용자명
            password: 비밀번호
        """
        # Neo4j 드라이버 생성 (auth는 튜플로 전달)
        self.driver = GraphDatabase.driver(
            uri, 
            auth=(user, password),
            max_connection_lifetime=3600
        )
        
        # 연결 테스트
        try:
            self.driver.verify_connectivity()
            logger.info(f"✅ Neo4j 연결 성공: {uri}")
        except Exception as e:
            logger.error(f"❌ Neo4j 연결 실패: {e}")
            raise
    
    def close(self):
        """드라이버 연결 종료"""
        self.driver.close()
    
    def clear(self):
        """데이터베이스 전체 초기화 (주의!)"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("⚠️  데이터베이스 초기화 완료")
    
    def create_indexes(self):
        """검색 성능을 위한 인덱스 생성"""
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.uid)",
                "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.article_id)",
                "CREATE INDEX IF NOT EXISTS FOR (a:Article) ON (a.law_code)",
                "CREATE INDEX IF NOT EXISTS FOR (c:Clause) ON (c.uid)",
                "CREATE INDEX IF NOT EXISTS FOR (i:Item) ON (i.uid)",
                "CREATE INDEX IF NOT EXISTS FOR (s:Subitem) ON (s.uid)",
            ]
            for idx in indexes:
                try:
                    session.run(idx)
                except Exception:
                    pass
        logger.info("✅ 인덱스 생성 완료")
    
    def build(self, data: Dict, law_def: LawDefinition):
        """
        그래프 구축 메인 함수
        
        Args:
            data: 파싱된 법령 데이터
            law_def: 법령 정의 객체
        """
        law_code = data['law_code']
        law_type = data['law_type']
        
        # Neo4j 레이블 선택
        if law_type == 'Act':
            label = law_def.act_label
        elif law_type == 'Decree':
            label = law_def.decree_label
        else:
            label = law_def.rule_label
        
        logger.info(f"🔨 그래프 구축 시작: {law_code}/{law_type} ({label})")
        
        with self.driver.session() as session:
            for art in data['articles']:
                self._build_article(session, art, law_code, law_type, label)
        
        logger.info(f"✅ 그래프 구축 완료: {law_code}/{law_type}")
    
    def _build_article(self, session, art: Dict, law_code: str, law_type: str, label: str):
        """
        조항 및 하위 구조(항, 호, 목) 노드 생성
        
        Args:
            session: Neo4j 세션
            art: 조항 데이터
            law_code: 법령 코드
            law_type: 법령 종류
            label: Neo4j 레이블
        """
        # 조항 고유 ID
        art_uid = f"{law_code}_{law_type}_{art['id']}"
        
        # 조항 노드 생성 (Article + 법령별 레이블)
        session.run(f"""
            MERGE (a:Article:{label} {{uid: $uid}})
            SET a.article_id = $id,
                a.title = $title,
                a.law_code = $law_code,
                a.law_type = $law_type,
                a.full_text = $text
        """, {
            'uid': art_uid,
            'id': art['id'],
            'title': art['title'],
            'law_code': law_code,
            'law_type': law_type,
            'text': art['text']
        })
        
        # 항(Clause) 처리
        for clause in art['clauses']:
            cls_uid = f"{art_uid}_{clause['id']}"
            
            session.run("""
                MERGE (c:Clause {uid: $uid})
                SET c.clause_id = $id,
                    c.content = $text,
                    c.law_code = $law_code,
                    c.law_type = $law_type
            """, {
                'uid': cls_uid,
                'id': clause['id'],
                'text': clause['text'],
                'law_code': law_code,
                'law_type': law_type
            })
            
            # 조항 → 항 관계
            session.run("""
                MATCH (a:Article {uid: $a})
                MATCH (c:Clause {uid: $c})
                MERGE (a)-[:CONTAINS]->(c)
            """, {'a': art_uid, 'c': cls_uid})
            
            # 호(Item) 처리
            for item in clause['items']:
                itm_uid = f"{cls_uid}_{item['id']}"
                
                session.run("""
                    MERGE (i:Item {uid: $uid})
                    SET i.item_id = $id,
                        i.content = $text,
                        i.law_code = $law_code,
                        i.law_type = $law_type
                """, {
                    'uid': itm_uid,
                    'id': item['id'],
                    'text': item['text'],
                    'law_code': law_code,
                    'law_type': law_type
                })
                
                # 항 → 호 관계
                session.run("""
                    MATCH (c:Clause {uid: $c})
                    MATCH (i:Item {uid: $i})
                    MERGE (c)-[:CONTAINS]->(i)
                """, {'c': cls_uid, 'i': itm_uid})
                
                # 목(Subitem) 처리
                for sub in item['subitems']:
                    sub_uid = f"{itm_uid}_{sub['id']}"
                    
                    session.run("""
                        MERGE (s:Subitem {uid: $uid})
                        SET s.subitem_id = $id,
                            s.content = $text,
                            s.law_code = $law_code,
                            s.law_type = $law_type
                    """, {
                        'uid': sub_uid,
                        'id': sub['id'],
                        'text': sub['text'],
                        'law_code': law_code,
                        'law_type': law_type
                    })
                    
                    # 호 → 목 관계
                    session.run("""
                        MATCH (i:Item {uid: $i})
                        MATCH (s:Subitem {uid: $s})
                        MERGE (i)-[:CONTAINS]->(s)
                    """, {'i': itm_uid, 's': sub_uid})
    
    def create_relations(self):
        """
        조항 간 참조/위임 관계 생성
        
        관계 종류:
        - REFERS_TO: 동일 법령 내 참조 (제N조 참조)
        - DELEGATES_TO: 위임 관계 (법→영, 영→규칙)
        - CROSS_REFERS_TO: 다른 법령 간 참조
        """
        logger.info("🔗 관계 생성 시작...")
        
        # 정규식 패턴
        p_internal = re.compile(r'제(\d+(?:의\d+)?)조')       # 내부 참조
        p_act_ref = re.compile(r'법\s*제(\d+(?:의\d+)?)조')   # 시행령에서 법 참조
        p_decree_ref = re.compile(r'영\s*제(\d+(?:의\d+)?)조') # 시행규칙에서 시행령 참조
        
        with self.driver.session() as session:
            # 모든 조항 가져오기
            result = session.run("""
                MATCH (a:Article)
                RETURN a.uid as uid, a.full_text as text, 
                       a.law_code as code, a.law_type as type
            """)
            
            for record in result:
                uid = record['uid']
                text = record['text'] or ""
                law_code = record['code']
                law_type = record['type']
                
                # 1) 내부 참조 (REFERS_TO)
                for match in p_internal.finditer(text):
                    target_id = f"제{match.group(1)}조"
                    target_uid = f"{law_code}_{law_type}_{target_id}"
                    
                    # 자기 자신 참조 제외
                    if target_uid != uid:
                        session.run("""
                            MATCH (a:Article {uid: $from})
                            MATCH (b:Article {uid: $to})
                            MERGE (a)-[:REFERS_TO]->(b)
                        """, {'from': uid, 'to': target_uid})
                
                # 2) 위임 관계 (DELEGATES_TO)
                if law_type == 'Decree':
                    # 시행령 → 법률 참조
                    for match in p_act_ref.finditer(text):
                        target_id = f"제{match.group(1)}조"
                        target_uid = f"{law_code}_Act_{target_id}"
                        session.run("""
                            MATCH (a:Article {uid: $from})
                            MATCH (b:Article {uid: $to})
                            MERGE (a)-[:DELEGATES_TO]->(b)
                        """, {'from': uid, 'to': target_uid})
                
                elif law_type == 'Rule':
                    # 시행규칙 → 시행령 참조
                    for match in p_decree_ref.finditer(text):
                        target_id = f"제{match.group(1)}조"
                        target_uid = f"{law_code}_Decree_{target_id}"
                        session.run("""
                            MATCH (a:Article {uid: $from})
                            MATCH (b:Article {uid: $to})
                            MERGE (a)-[:DELEGATES_TO]->(b)
                        """, {'from': uid, 'to': target_uid})
        
        logger.info("✅ 관계 생성 완료")
    
    def print_stats(self):
        """그래프 통계 출력"""
        with self.driver.session() as session:
            # 법령별 조항 수
            law_stats = session.run("""
                MATCH (a:Article)
                RETURN a.law_code as law, a.law_type as type, count(a) as cnt
                ORDER BY a.law_code, a.law_type
            """).data()
            
            # 전체 노드 수
            total_articles = session.run("MATCH (a:Article) RETURN count(a)").single()[0]
            total_clauses = session.run("MATCH (c:Clause) RETURN count(c)").single()[0]
            total_items = session.run("MATCH (i:Item) RETURN count(i)").single()[0]
            total_subitems = session.run("MATCH (s:Subitem) RETURN count(s)").single()[0]
            
            # 관계 수
            contains = session.run("MATCH ()-[r:CONTAINS]->() RETURN count(r)").single()[0]
            refers = session.run("MATCH ()-[r:REFERS_TO]->() RETURN count(r)").single()[0]
            delegates = session.run("MATCH ()-[r:DELEGATES_TO]->() RETURN count(r)").single()[0]
            
            # 통계 출력
            msg = []
            msg.append("\n" + "="*70)
            msg.append("📊 Knowledge Graph 통계")
            msg.append("="*70)
            
            msg.append("\n[법령별 조항 수]")
            current_law = None
            for stat in law_stats:
                if stat['law'] != current_law:
                    if current_law:
                        msg.append("")
                    current_law = stat['law']
                    law_name = LAWS.get(stat['law'], LawDefinition(code=stat['law'], name=stat['law'], 
                                        act_label='', decree_label='', rule_label='', pdf_paths={})).name
                    msg.append(f"\n{law_name}")
                msg.append(f"  {stat['type']:8s}: {stat['cnt']:4d}개")
            
            msg.append(f"\n\n[전체 노드]")
            msg.append(f"  Article  : {total_articles:,}")
            msg.append(f"  Clause   : {total_clauses:,}")
            msg.append(f"  Item     : {total_items:,}")
            msg.append(f"  Subitem  : {total_subitems:,}")
            msg.append(f"  합계     : {total_articles+total_clauses+total_items+total_subitems:,}")
            
            msg.append(f"\n[관계]")
            msg.append(f"  CONTAINS    : {contains:,}")
            msg.append(f"  REFERS_TO   : {refers:,}")
            msg.append(f"  DELEGATES_TO: {delegates:,}")
            msg.append(f"  합계        : {contains+refers+delegates:,}")
            msg.append("="*70 + "\n")
            
            logger.info('\n'.join(msg))


# ==========================================
# 환경변수 로드 함수
# ==========================================
def load_env_file(env_path: str = ".env") -> Dict[str, str]:
    """
    .env 파일에서 환경변수를 읽어오는 함수
    
    Args:
        env_path: .env 파일 경로 (기본값: 현재 디렉토리의 .env)
        
    Returns:
        환경변수 딕셔너리
    """
    env_vars = {}
    
    if not os.path.exists(env_path):
        logger.warning(f"⚠️  .env 파일이 없습니다: {env_path}")
        return env_vars
    
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            
            # 빈 줄이나 주석 무시
            if not line or line.startswith('#'):
                continue
            
            # KEY=VALUE 형식 파싱
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                # 따옴표 제거 (시작과 끝이 같은 따옴표인 경우)
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]
                
                env_vars[key] = value
                # 환경변수로도 설정
                os.environ[key] = value
    
    logger.info(f"✅ .env 파일 로드 완료: {len(env_vars)}개 변수")
    return env_vars


# ==========================================
# 메인 실행 함수
# ==========================================
def main(
    law_codes: Optional[List[str]] = None,
    clear_db: bool = True,
    env_path: str = ".env"
):
    """
    Knowledge Graph 구축 메인 함수
    
    .env 파일에서 다음 환경변수를 읽어옵니다:
    - NEO4J_URI: Neo4j 서버 URI (기본값: bolt://localhost:7687)
    - NEO4J_USER: Neo4j 사용자명 (기본값: neo4j)
    - NEO4J_PASSWORD: Neo4j 비밀번호 (필수)
    - UPSTAGE_API_KEY: Upstage API 키 (필수)
    
    Args:
        law_codes: 처리할 법령 코드 목록 (None이면 전체)
        clear_db: 시작 전 DB 초기화 여부
        env_path: .env 파일 경로
    """
    logger.info("="*70)
    logger.info(f"🚀 법령 Knowledge Graph 구축 시작")
    logger.info(f"   시간: {datetime.datetime.now()}")
    logger.info("="*70)
    
    # .env 파일에서 환경변수 로드
    load_env_file(env_path)
    
    # 환경변수에서 설정값 가져오기
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    upstage_api_key = os.getenv("UPSTAGE_API_KEY")
    
    # 필수 환경변수 확인
    if not neo4j_password:
        logger.error("❌ NEO4J_PASSWORD 환경변수가 설정되지 않았습니다.")
        logger.error("   .env 파일에 NEO4J_PASSWORD=your_password 형식으로 추가하세요.")
        return
    
    if not upstage_api_key:
        logger.error("❌ UPSTAGE_API_KEY 환경변수가 설정되지 않았습니다.")
        logger.error("   .env 파일에 UPSTAGE_API_KEY=your_api_key 형식으로 추가하세요.")
        return
    
    logger.info(f"📌 Neo4j URI: {neo4j_uri}")
    logger.info(f"📌 Neo4j User: {neo4j_user}")
    logger.info(f"📌 Upstage API Key: {upstage_api_key[:8]}...")
    
    # 1. Upstage 추출기 초기화
    try:
        extractor = UpstageDocumentExtractor(api_key=upstage_api_key)
    except ValueError as e:
        logger.error(f"❌ Upstage 초기화 실패: {e}")
        return
    
    # 2. 파서 초기화
    parser = LawParser()
    
    # 3. Neo4j 빌더 초기화
    try:
        logger.info(f"🔌 Neo4j 연결 시도 중... ({neo4j_uri})")
        builder = GraphBuilder(neo4j_uri, neo4j_user, neo4j_password)
    except Exception as e:
        logger.error(f"❌ Neo4j 연결 실패: {e}")
        logger.error("   다음 사항을 확인해주세요:")
        logger.error("   1. Neo4j 서버가 실행 중인지 확인")
        logger.error("   2. NEO4J_URI가 올바른지 확인 (예: bolt://localhost:7687)")
        logger.error("   3. NEO4J_USER와 NEO4J_PASSWORD가 올바른지 확인")
        logger.error("   4. neo4j 패키지 버전 확인: pip install --upgrade neo4j")
        return
    
    try:
        # DB 초기화
        # if clear_db:
        #     builder.clear()
        
        # 인덱스 생성
        builder.create_indexes()
        
        # 처리할 법령 선택
        laws_to_process = law_codes if law_codes else list(LAWS.keys())
        
        # 4. 각 법령 처리
        for law_code in laws_to_process:
            if law_code not in LAWS:
                logger.warning(f"⚠️  알 수 없는 법령 코드: {law_code}")
                continue
            
            law_def = LAWS[law_code]
            logger.info(f"\n{'='*50}")
            logger.info(f"📚 {law_def.name} 처리 시작")
            logger.info(f"{'='*50}")
            
            # 법률, 시행령, 시행규칙 순서로 처리
            for law_type, pdf_path in law_def.pdf_paths.items():
                if not os.path.exists(pdf_path):
                    logger.warning(f"⚠️  파일 없음: {pdf_path}")
                    continue
                
                # PDF 텍스트 추출 (Upstage API 사용)
                text = extractor.extract(pdf_path)
                if not text:
                    continue
                
                # 법령 파싱
                data = parser.parse(text, law_code, law_type)
                
                # 그래프 구축
                builder.build(data, law_def)
        
        # 5. 관계 생성
        builder.create_relations()
        
        # 6. 통계 출력
        builder.print_stats()
        
        logger.info("✅ 모든 작업 완료!")
        
    finally:
        builder.close()


# ==========================================
# 실행
# ==========================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='법령 Knowledge Graph 구축 (Upstage 버전)')
    parser.add_argument('--env', default='.env', help='.env 파일 경로 (기본값: .env)')
    parser.add_argument('--laws', nargs='+', default=None, help='처리할 법령 코드 (예: BUILDING BUILDING_MGMT)')
    parser.add_argument('--no-clear', action='store_true', help='DB 초기화 안 함')
    
    args = parser.parse_args()
    
    main(
        law_codes=args.laws,
        clear_db=not args.no_clear,
        env_path=args.env
    )