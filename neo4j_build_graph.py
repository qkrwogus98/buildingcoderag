"""
건축 관련 법령 Graph RAG - 다중 법령 지원 버전
건축법, 건축물관리법, 주택법, 국토계획법, 주차장법, 건축서비스산업진흥법
"""

import pdfplumber
import re
import os
from neo4j import GraphDatabase
from typing import List, Dict, Tuple
import logging
import datetime
from dataclasses import dataclass

# 기존 logging 설정 대신 아래 함수를 사용
def setup_custom_logger():
    # 로거 가져오기
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 기존 핸들러가 있다면 제거 (중복 출력 방지)
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # 포맷터 설정
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    
    # 1. 콘솔 출력용 핸들러 (StreamHandler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # 2. 파일 저장용 핸들러 (FileHandler) - mode='a' (append)
    # encoding='utf-8'을 넣어 한글 깨짐 방지
    file_handler = logging.FileHandler('build_graph_log.txt', mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_custom_logger()

# ==========================================
# 법령 정의
# ==========================================
@dataclass
class LawDefinition:
    """법령 정의"""
    code: str           # 짧은 코드 (예: BUILDING, HOUSING)
    name: str           # 법령명 (예: 건축법, 주택법)
    act_label: str      # Neo4j 레이블 (예: BuildingAct)
    decree_label: str
    rule_label: str
    pdf_paths: Dict[str, str]  # {'Act': 'path', 'Decree': 'path', 'Rule': 'path'}


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
# PDF 추출
# ==========================================
def extract_text_from_pdf(pdf_path: str, skip_toc=True) -> str:
    """PDF에서 텍스트 추출 (목차 제외)"""
    if not os.path.exists(pdf_path):
        logger.warning(f"파일 없음: {pdf_path}")
        return ""
    
    logger.info(f"📄 {os.path.basename(pdf_path)}")
    
    def is_toc_page(text: str) -> bool:
        """목차 페이지 판별"""
        if not text:
            return True
        lines = text.split('\n')
        article_lines = [l for l in lines if re.match(r'^\s*제\d+조', l)]
        has_clauses = bool(re.search(r'[①②③④⑤]', text))
        return len(article_lines) > 15 and not has_clauses
    
    text = ""
    skipped = 0
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if not page_text:
                    continue
                
                if skip_toc and is_toc_page(page_text):
                    skipped += 1
                    continue
                
                # 노이즈 제거
                lines = []
                for line in page_text.split('\n'):
                    line = line.strip()
                    if line in ["법제처", "국가법령정보센터"] or line.endswith("법"):
                        continue
                    if line.isdigit() or re.match(r'^법제처\s+\d+', line):
                        continue
                    lines.append(line)
                
                text += '\n'.join(lines) + "\n"
        
        logger.info(f"✅ {len(text):,}자 (목차 {skipped}p 제외)")
        return text
        
    except Exception as e:
        logger.error(f"PDF 추출 실패 {pdf_path}: {e}")
        return ""


# ==========================================
# 법령 파서
# ==========================================
class LawParser:
    """법령 파서"""
    
    def parse(self, text: str, law_code: str, law_type: str) -> Dict:
        """
        법령 파싱
        
        Args:
            text: PDF 텍스트
            law_code: 법령 코드 (예: BUILDING, HOUSING)
            law_type: 법령 종류 (Act, Decree, Rule)
        
        Returns:
            파싱된 데이터
        """
        articles_data = []
        
        pattern = re.compile(r'\n(제\d+조(?:의\d+)?)\(([^)]+)\)')
        matches = list(pattern.finditer(text))
        
        for i, match in enumerate(matches):
            article_id = match.group(1)
            title = match.group(2)
            
            start = match.start()
            end = matches[i+1].start() if i+1 < len(matches) else len(text)
            article_text = text[start:end].strip()
            
            clauses = self._parse_clauses(article_text)
            
            # 본문이 있는 조항만
            if clauses or len(article_text) > 100:
                articles_data.append({
                    'id': article_id,
                    'title': title,
                    'text': article_text,
                    'clauses': clauses
                })
        
        # 중복 제거
        unique = {}
        for art in articles_data:
            aid = art['id']
            if aid not in unique or len(art['text']) > len(unique[aid]['text']):
                unique[aid] = art
        
        result = list(unique.values())
        logger.info(f"✅ {law_code}-{law_type}: {len(result)}개 조항")
        
        return {
            'law_code': law_code,
            'law_type': law_type,
            'articles': result
        }
    
    def _parse_clauses(self, text: str) -> List[Dict]:
        """항 파싱"""
        clauses = []
        parts = re.split(r'([①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳])', text)
        
        i = 0
        while i < len(parts):
            part = parts[i]
            if re.match(r'[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]', part):
                if i + 1 < len(parts):
                    clause_text = parts[i+1].strip()
                    if clause_text:
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
        """호 파싱"""
        items = []
        lines = text.split('\n')
        current_item = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            match = re.match(r'^(\d+)\.\s', line)
            if match:
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
        """목 파싱"""
        subitems = []
        lines = text.split('\n')
        current_sub = None
        current_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
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
# Neo4j Graph Builder
# ==========================================
class GraphBuilder:
    """Neo4j 그래프 구축"""
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"✅ Neo4j 연결")
    
    def close(self):
        self.driver.close()
    
    def clear(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("⚠️  DB 초기화")
    
    def create_indexes(self):
        """인덱스 생성"""
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
                except:
                    pass
        logger.info("✅ 인덱스")
    
    def build(self, data: Dict, law_def: LawDefinition):
        """
        그래프 구축
        
        Args:
            data: 파싱된 데이터
            law_def: 법령 정의
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
        
        logger.info(f"🔨 {law_code}-{law_type} ({label}) 구축...")
        
        with self.driver.session() as session:
            for art in data['articles']:
                self._build_article(session, art, law_code, law_type, label)
        
        logger.info(f"✅ {law_code}-{law_type}")
    
    def _build_article(self, session, art: Dict, law_code: str, law_type: str, label: str):
        """조항 및 하위 구조 생성"""
        art_uid = f"{law_code}_{law_type}_{art['id']}"
        
        # 조항 노드 (Article + 법령별 레이블)
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
        
        # 항
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
            
            session.run("""
                MATCH (a:Article {uid: $a})
                MATCH (c:Clause {uid: $c})
                MERGE (a)-[:CONTAINS]->(c)
            """, {'a': art_uid, 'c': cls_uid})
            
            # 호
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
                
                session.run("""
                    MATCH (c:Clause {uid: $c})
                    MATCH (i:Item {uid: $i})
                    MERGE (c)-[:CONTAINS]->(i)
                """, {'c': cls_uid, 'i': itm_uid})
                
                # 목
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
                    
                    session.run("""
                        MATCH (i:Item {uid: $i})
                        MATCH (s:Subitem {uid: $s})
                        MERGE (i)-[:CONTAINS]->(s)
                    """, {'i': itm_uid, 's': sub_uid})
    
    def create_relations(self):
        """조항 간 관계 생성 (Python 정규식 기반 개선 버전)"""
        logger.info("🔗 관계 생성...")
        
        # 정규식 패턴 컴파일
        # 1. 내부 참조: "제N조" 또는 "제N조의N"
        p_internal = re.compile(r'제(\d+(?:의\d+)?)조')
        
        # 2. 위임 관계 (법/영): "법 제N조", "영 제N조" (띄어쓰기 포함)
        p_act_ref = re.compile(r'법\s*제(\d+(?:의\d+)?)조')    # 령 -> 법
        p_decree_ref = re.compile(r'영\s*제(\d+(?:의\d+)?)조') # 규칙 -> 령
        
        # 3. 외부 법령 참조: "법령명 + (공백) + 제N조" 형태만 엄격하게 매칭
        # 예: "건축물관리법 제39조" (O), "건축물관리법 ... 제39조" (X - 오탐지 방지)
        cross_patterns = {
            'BUILDING': [
                ('주택법', 'HOUSING'),
                ('건축물관리법', 'BUILDING_MGMT'),
                ('국토의계획및이용에관한법률', 'LAND_PLAN'),
                ('주차장법', 'PARKING')
            ],
            'HOUSING': [('건축법', 'BUILDING')],
            'LAND_PLAN': [('건축법', 'BUILDING')]
        }

        with self.driver.session() as session:
            # 모든 Article을 메모리로 가져와서 처리 (속도 및 정확성 향상)
            result = session.run("""
                MATCH (a:Article) 
                RETURN a.uid as uid, a.law_code as law_code, a.law_type as law_type, 
                       a.article_id as article_id, a.full_text as text
            """)
            
            articles = [record for record in result]
            total = len(articles)
            logger.info(f"🔍 총 {total}개 조항 분석 시작...")

            rels_internal = []
            rels_delegates = []
            rels_cross = []

            for idx, r in enumerate(articles):
                uid = r['uid']
                text = r['text']
                curr_code = r['law_code']
                curr_type = r['law_type']
                curr_id = r['article_id'] # 예: 제1조

                # 1. 같은 법령 내 참조 (REFERS_TO)
                # "제5조에 따라" -> "제5조" 추출
                for match in p_internal.finditer(text):
                    ref_num = match.group(1)
                    ref_id = f"제{ref_num}조"
                    
                    # 자기 자신 참조 제외
                    if ref_id != curr_id:
                        rels_internal.append({
                            'from': uid,
                            'to_code': curr_code,
                            'to_type': curr_type,
                            'to_id': ref_id
                        })

                # 2. 위임 관계 (DELEGATES_TO)
                # Decree(시행령) -> Act(법) : "법 제X조"
                if curr_type == 'Decree':
                    for match in p_act_ref.finditer(text):
                        ref_num = match.group(1)
                        rels_delegates.append({
                            'from': uid,
                            'to_code': curr_code,
                            'to_type': 'Act',
                            'to_id': f"제{ref_num}조"
                        })
                
                # Rule(시행규칙) -> Decree(시행령) : "영 제X조"
                elif curr_type == 'Rule':
                    for match in p_decree_ref.finditer(text):
                        ref_num = match.group(1)
                        rels_delegates.append({
                            'from': uid,
                            'to_code': curr_code,
                            'to_type': 'Decree',
                            'to_id': f"제{ref_num}조"
                        })

                # 3. 타 법령 참조 (CROSS_REFERS_TO)
                if curr_code in cross_patterns:
                    for kw, target_code in cross_patterns[curr_code]:
                        # 정확히 "법령명 제N조" 패턴인 경우만 찾음
                        # 예: r"건축물관리법\s+제(\d+)조"
                        p_cross = re.compile(re.escape(kw) + r'\s*제(\d+(?:의\d+)?)조')
                        for match in p_cross.finditer(text):
                            ref_num = match.group(1)
                            rels_cross.append({
                                'from': uid,
                                'to_code': target_code,
                                'to_id': f"제{ref_num}조"
                            })

            # 배치 처리를 위한 헬퍼 함수
            def batch_run(query, data_list, batch_size=1000):
                if not data_list: return
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i+batch_size]
                    session.run(query, {'batch': batch})

            # 1. REFERS_TO 저장
            logger.info(f"💾 REFERS_TO {len(rels_internal)}개 저장 중...")
            q_internal = """
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                MATCH (t:Article {law_code: row.to_code, law_type: row.to_type, article_id: row.to_id})
                MERGE (a)-[:REFERS_TO]->(t)
            """
            batch_run(q_internal, rels_internal)

            # 2. DELEGATES_TO 저장
            logger.info(f"💾 DELEGATES_TO {len(rels_delegates)}개 저장 중...")
            q_delegates = """
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                MATCH (t:Article {law_code: row.to_code, law_type: row.to_type, article_id: row.to_id})
                MERGE (a)-[:DELEGATES_TO]->(t)
            """
            batch_run(q_delegates, rels_delegates)

            # 3. CROSS_REFERS_TO 저장
            logger.info(f"💾 CROSS_REFERS_TO {len(rels_cross)}개 저장 중...")
            q_cross = """
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                # 타 법령은 Act(법률)를 참조하는 것이 일반적이므로 law_type: 'Act'로 고정하거나 필요시 수정
                MATCH (t:Article {law_code: row.to_code, law_type: 'Act', article_id: row.to_id})
                MERGE (a)-[:CROSS_REFERS_TO]->(t)
            """
            batch_run(q_cross, rels_cross)

        logger.info("✅ 관계 생성 완료")
    
    def stats(self):
        """통계 출력"""
        with self.driver.session() as session:
            # 법령별 통계
            law_stats = session.run("""
                MATCH (a:Article)
                RETURN a.law_code as law, a.law_type as type, count(a) as cnt
                ORDER BY law, type
            """).data()
            
            total_articles = session.run("MATCH (a:Article) RETURN count(a)").single()[0]
            total_clauses = session.run("MATCH (c:Clause) RETURN count(c)").single()[0]
            total_items = session.run("MATCH (i:Item) RETURN count(i)").single()[0]
            total_subitems = session.run("MATCH (s:Subitem) RETURN count(s)").single()[0]
            
            contains = session.run("MATCH ()-[r:CONTAINS]->() RETURN count(r)").single()[0]
            refers = session.run("MATCH ()-[r:REFERS_TO]->() RETURN count(r)").single()[0]
            delegates = session.run("MATCH ()-[r:DELEGATES_TO]->() RETURN count(r)").single()[0]
            cross_refers = session.run("MATCH ()-[r:CROSS_REFERS_TO]->() RETURN count(r)").single()[0]
            
            # 로그 메시지 구성
            msg = []
            msg.append("\n" + "="*70)
            msg.append("📊 Graph Database 통계")
            msg.append("="*70)
            
            msg.append("\n[법령별 조항 수]")
            current_law = None
            for stat in law_stats:
                if stat['law'] != current_law:
                    if current_law:
                        msg.append("")
                    current_law = stat['law']
                    msg.append(f"\n{LAWS[stat['law']].name}")
                msg.append(f"  {stat['type']:8s}: {stat['cnt']:4d}개")
            
            msg.append(f"\n\n전체 노드:")
            msg.append(f"  Article  : {total_articles:,}")
            msg.append(f"  Clause   : {total_clauses:,}")
            msg.append(f"  Item     : {total_items:,}")
            msg.append(f"  Subitem  : {total_subitems:,}")
            msg.append(f"  합계     : {total_articles+total_clauses+total_items+total_subitems:,}")
            
            msg.append(f"\n[관계:]")
            msg.append(f"  CONTAINS       : {contains:,}")
            msg.append(f"  REFERS_TO      : {refers:,}")
            msg.append(f"  DELEGATES_TO   : {delegates:,}")
            msg.append(f"  CROSS_REFERS_TO: {cross_refers:,}")
            msg.append(f"  합계           : {contains+refers+delegates+cross_refers:,}")
            msg.append("="*70 + "\n")


            logger.info('\n'.join(msg))


# ==========================================
# 메인 실행
# ==========================================
def main():
    """메인 실행"""
    # 실행 시각 기록 (로그 파일 구분용)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n\n{'#'*70}")
    logger.info(f"🚀 실행 시작: {now_str}")
    logger.info(f"{'#'*70}\n")

    # Neo4j 설정
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "DxI3O9BGnjGjdgu800HRd8kewNhHU9URb6lCMn3V4XI"
    
    # 처리할 법령 선택, 필요시 수정
    # 우선은 2개 법령만 처리
    laws_to_process = ['BUILDING', 'BUILDING_MGMT', 'BUILDING_SERVICE', 'PARKING', 'LAND_PLAN', 'HOUSING', 'GREEN_BUILDING',
'HANOK', 'BUILDING_SALE', 'CONVENIENCE']

    
    parser = LawParser()
    builder = GraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # builder.clear()  # 기존 데이터 삭제 (선택)
        builder.create_indexes()
        
        # 각 법령 처리
        for law_code in laws_to_process:
            law_def = LAWS[law_code]
            
            logger.info(f"\n{'='*70}")
            logger.info(f"{law_def.name} 처리")
            logger.info(f"{'='*70}")
            
            # 법/령/규칙 각각 처리
            for law_type, pdf_path in law_def.pdf_paths.items():
                if not os.path.exists(pdf_path):
                    logger.warning(f"⚠️  파일 없음: {pdf_path}")
                    continue
                
                text = extract_text_from_pdf(pdf_path, skip_toc=True)
                if text:
                    parsed = parser.parse(text, law_code, law_type)
                    builder.build(parsed, law_def)
        
        # 관계 생성
        logger.info(f"\n{'='*70}")
        builder.create_relations()
        
        # 통계
        builder.stats()
        
        logger.info("✅ 모든 작업 완료!")
        
    except Exception as e:
        logger.error(f"❌ {e}", exc_info=True)
    finally:
        builder.close()


if __name__ == "__main__":
    main()