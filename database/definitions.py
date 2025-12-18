# definitions.py
from dataclasses import dataclass
from typing import Dict

@dataclass
class LawDefinition:
    """법령 정의"""
    code: str           # 짧은 코드 (예: BUILDING)
    name: str           # 법령명
    act_label: str      # Neo4j 레이블 (Act)
    decree_label: str   # Neo4j 레이블 (Decree)
    rule_label: str     # Neo4j 레이블 (Rule)
    pdf_paths: Dict[str, str]  # {'Act': '...', 'Decree': '...', 'Rule': '...'}

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
    # 필요한 법령 추가...
}