# graph_builder.py
import logging
import re
from typing import Dict
from neo4j import GraphDatabase
from definitions import LawDefinition, LAWS

logger = logging.getLogger(__name__)

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
        logger.info("✅ 인덱스 생성")
    
    def build(self, data: Dict, law_def: LawDefinition):
        law_code = data['law_code']
        law_type = data['law_type']
        
        if law_type == 'Act': label = law_def.act_label
        elif law_type == 'Decree': label = law_def.decree_label
        else: label = law_def.rule_label
        
        logger.info(f"🔨 {law_code}-{law_type} ({label}) 구축...")
        
        with self.driver.session() as session:
            for art in data['articles']:
                self._build_article(session, art, law_code, law_type, label)
        
        logger.info(f"✅ {law_code}-{law_type}")
    
    def _build_article(self, session, art: Dict, law_code: str, law_type: str, label: str):
        art_uid = f"{law_code}_{law_type}_{art['id']}"
        session.run(f"""
            MERGE (a:Article:{label} {{uid: $uid}})
            SET a.article_id = $id, a.title = $title, a.law_code = $law_code, a.law_type = $law_type, a.full_text = $text
        """, {'uid': art_uid, 'id': art['id'], 'title': art['title'], 'law_code': law_code, 'law_type': law_type, 'text': art['text']})
        
        for clause in art['clauses']:
            cls_uid = f"{art_uid}_{clause['id']}"
            session.run("""
                MERGE (c:Clause {uid: $uid})
                SET c.clause_id = $id, c.content = $text, c.law_code = $code, c.law_type = $type
            """, {'uid': cls_uid, 'id': clause['id'], 'text': clause['text'], 'code': law_code, 'type': law_type})
            session.run("MATCH (a:Article {uid: $a}) MATCH (c:Clause {uid: $c}) MERGE (a)-[:CONTAINS]->(c)", {'a': art_uid, 'c': cls_uid})
            
            for item in clause['items']:
                itm_uid = f"{cls_uid}_{item['id']}"
                session.run("""
                    MERGE (i:Item {uid: $uid})
                    SET i.item_id = $id, i.content = $text, i.law_code = $code, i.law_type = $type
                """, {'uid': itm_uid, 'id': item['id'], 'text': item['text'], 'code': law_code, 'type': law_type})
                session.run("MATCH (c:Clause {uid: $c}) MATCH (i:Item {uid: $i}) MERGE (c)-[:CONTAINS]->(i)", {'c': cls_uid, 'i': itm_uid})
                
                for sub in item['subitems']:
                    sub_uid = f"{itm_uid}_{sub['id']}"
                    session.run("""
                        MERGE (s:Subitem {uid: $uid})
                        SET s.subitem_id = $id, s.content = $text, s.law_code = $code, s.law_type = $type
                    """, {'uid': sub_uid, 'id': sub['id'], 'text': sub['text'], 'code': law_code, 'type': law_type})
                    session.run("MATCH (i:Item {uid: $i}) MATCH (s:Subitem {uid: $s}) MERGE (i)-[:CONTAINS]->(s)", {'i': itm_uid, 's': sub_uid})

    def create_relations(self):
        logger.info("🔗 관계 생성...")
        p_internal = re.compile(r'제(\d+(?:의\d+)?)조')
        p_act_ref = re.compile(r'법\s*제(\d+(?:의\d+)?)조')
        p_decree_ref = re.compile(r'영\s*제(\d+(?:의\d+)?)조')
        
        cross_patterns = {
            'BUILDING': [('주택법', 'HOUSING'), ('건축물관리법', 'BUILDING_MGMT'), ('국토의계획및이용에관한법률', 'LAND_PLAN'), ('주차장법', 'PARKING')],
            'HOUSING': [('건축법', 'BUILDING')],
            'LAND_PLAN': [('건축법', 'BUILDING')]
        }

        with self.driver.session() as session:
            result = session.run("MATCH (a:Article) RETURN a.uid as uid, a.law_code as law_code, a.law_type as law_type, a.article_id as article_id, a.full_text as text")
            articles = [record for record in result]
            logger.info(f"🔍 총 {len(articles)}개 조항 분석 시작...")

            rels_internal, rels_delegates, rels_cross = [], [], []

            for r in articles:
                uid, text, curr_code, curr_type, curr_id = r['uid'], r['text'], r['law_code'], r['law_type'], r['article_id']
                
                # 1. 내부 참조
                for match in p_internal.finditer(text):
                    ref_id = f"제{match.group(1)}조"
                    if ref_id != curr_id:
                        rels_internal.append({'from': uid, 'to_code': curr_code, 'to_type': curr_type, 'to_id': ref_id})

                # 2. 위임 관계
                if curr_type == 'Decree':
                    for match in p_act_ref.finditer(text):
                        rels_delegates.append({'from': uid, 'to_code': curr_code, 'to_type': 'Act', 'to_id': f"제{match.group(1)}조"})
                elif curr_type == 'Rule':
                    for match in p_decree_ref.finditer(text):
                        rels_delegates.append({'from': uid, 'to_code': curr_code, 'to_type': 'Decree', 'to_id': f"제{match.group(1)}조"})

                # 3. 외부 참조
                if curr_code in cross_patterns:
                    for kw, target_code in cross_patterns[curr_code]:
                        p_cross = re.compile(re.escape(kw) + r'\s*제(\d+(?:의\d+)?)조')
                        for match in p_cross.finditer(text):
                            rels_cross.append({'from': uid, 'to_code': target_code, 'to_id': f"제{match.group(1)}조"})

            def batch_run(query, data_list):
                if not data_list: return
                for i in range(0, len(data_list), 1000):
                    session.run(query, {'batch': data_list[i:i+1000]})

            logger.info(f"💾 REFERS_TO 저장...")
            batch_run("""
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                MATCH (t:Article {law_code: row.to_code, law_type: row.to_type, article_id: row.to_id})
                MERGE (a)-[:REFERS_TO]->(t)
            """, rels_internal)
            
            logger.info(f"💾 DELEGATES_TO 저장...")
            batch_run("""
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                MATCH (t:Article {law_code: row.to_code, law_type: row.to_type, article_id: row.to_id})
                MERGE (a)-[:DELEGATES_TO]->(t)
            """, rels_delegates)
            
            logger.info(f"💾 CROSS_REFERS_TO 저장...")
            batch_run("""
                UNWIND $batch as row
                MATCH (a:Article {uid: row.from})
                MATCH (t:Article {law_code: row.to_code, law_type: 'Act', article_id: row.to_id})
                MERGE (a)-[:CROSS_REFERS_TO]->(t)
            """, rels_cross)

        logger.info("✅ 관계 생성 완료")

    def stats(self):
        with self.driver.session() as session:
            law_stats = session.run("MATCH (a:Article) RETURN a.law_code as law, a.law_type as type, count(a) as cnt ORDER BY law, type").data()
            counts = session.run("""
                RETURN 
                count{ MATCH (a:Article) } as articles,
                count{ MATCH (c:Clause) } as clauses,
                count{ MATCH (i:Item) } as items,
                count{ MATCH (s:Subitem) } as subitems,
                count{ MATCH ()-[r:CONTAINS]->() } as contains,
                count{ MATCH ()-[r:REFERS_TO]->() } as refers,
                count{ MATCH ()-[r:DELEGATES_TO]->() } as delegates,
                count{ MATCH ()-[r:CROSS_REFERS_TO]->() } as cross
            """).single()
            
            msg = ["\n" + "="*70, "📊 Graph Database 통계", "="*70, "\n[법령별 조항 수]"]
            curr = None
            for s in law_stats:
                if s['law'] != curr:
                    curr = s['law']
                    msg.append(f"\n{LAWS.get(s['law'], type('obj', (object,), {'name': s['law']})).name}")
                msg.append(f"  {s['type']:8s}: {s['cnt']:4d}개")
            
            msg.append(f"\n[노드 합계]: {counts['articles']+counts['clauses']+counts['items']+counts['subitems']:,}")
            msg.append(f"[관계 합계]: {counts['contains']+counts['refers']+counts['delegates']+counts['cross']:,}")
            msg.append("="*70)
            logger.info('\n'.join(msg))