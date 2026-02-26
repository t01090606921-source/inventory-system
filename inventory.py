import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error("⚠️ Supabase 접속 설정(Secrets)을 확인해주세요.")

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 통합 관리 시스템 (최종 검증 완료)")

def sanitize_code(val):
    if pd.isna(val) or str(val).strip() == "": return ""
    return str(val).split('.')[0].strip().upper()

# --- 2. 사이드바 관리 ---
with st.sidebar:
    st.header("⚙️ 시스템 엔진 관리")
    if st.button("🔍 현재 DB 데이터 총 개수 확인", use_container_width=True):
        res = supabase.table("as_history").select("id", count="exact").limit(1).execute()
        st.metric("저장된 데이터", f"{res.count if res.count else 0:,} 건")

# --- 3. 메인 기능 탭 ---
tab0, tab1, tab2, tab3 = st.tabs(["🗂️ 마스터 관리", "📥 고속 입고", "📤 출고 처리", "📈 분석 리포트"])

# --- [TAB 0] 마스터 관리 (업로드 박스 강제 노출) ---
with tab0:
    st.subheader("📋 마스터 기준 정보 등록")
    st.markdown("자재번호별 **공급업체** 및 **분류구분** 정보를 시스템에 등록합니다.")
    
    # 이 부분에 파일 업로드 박스가 나타납니다.
    m_file = st.file_uploader("마스터 엑셀(XLSX) 또는 CSV 파일을 선택하세요", type=['xlsx', 'csv'], key="final_master_up")
    
    if m_file:
        if st.button("🔄 마스터 데이터 로드 시작", use_container_width=True):
            try:
                msg = st.empty()
                msg.info("⌛ 파일을 읽고 분석하는 중입니다...")
                
                if m_file.name.endswith('.csv'):
                    m_df = pd.read_csv(m_file, encoding='cp949').fillna("")
                else:
                    m_df = pd.read_excel(m_file).fillna("")
                
                # 데이터 맵핑 (0:자재번호, 5:업체명, 10:분류)
                st.session_state.master_lookup = {sanitize_code(row.iloc[0]): {
                    "업체": str(row.iloc[5]).strip() if len(row) > 5 else "미등록",
                    "분류": str(row.iloc[10]).strip() if len(row) > 10 else "수리대상"
                } for _, row in m_df.iterrows() if not pd.isna(row.iloc[0])}
                
                msg.success(f"✅ 마스터 데이터 {len(st.session_state.master_lookup):,}건 로드 완료!")
            except Exception as e:
                st.error(f"마스터 로드 오류: {e}")

# --- [TAB 1] 입고 처리 (진행 사항 모니터링) ---
with tab1:
    st.subheader("📥 신규 AS 입고 처리")
    i_file = st.file_uploader("AS 입고 CSV 파일 업로드", type=['csv'], key="final_in_up")
    
    if i_file and st.button("🚀 입고 프로세스 시작", use_container_width=True):
        if "master_lookup" not in st.session_state:
            st.error("⚠️ [마스터 관리] 탭에서 마스터 정보를 먼저 로드해주세요.")
        else:
            ui_msg, ui_prog = st.empty(), st.progress(0)
            try:
                # 1. 기존 데이터 로드 (중복 체크)
                existing_combos = set()
                offset, batch_size = 0, 4000
                while True:
                    res = supabase.table("as_history").select("입고일, 압축코드").range(offset, offset + batch_size - 1).execute()
                    if not res.data: break
                    for r in res.data:
                        existing_combos.add(f"{pd.to_datetime(r['입고일']).strftime('%Y-%m-%d')}|{str(r['압축코드']).strip().upper()}")
                    offset += len(res.data)
                    ui_msg.info(f"🔍 [1/2] 중복 확인용 DB 데이터 수집 중... ({offset:,} 건)")
                    if len(res.data) < batch_size: break

                # 2. 입고 파일 분석 및 저장
                for enc in ['utf-8-sig', 'cp949', 'utf-8']:
                    try: i_file.seek(0); i_df = pd.read_csv(i_file, encoding=enc).fillna(""); break
                    except: continue

                combined = i_df.astype(str).apply(lambda x: "".join(x), axis=1)
                as_in = i_df[combined.str.replace(" ", "").str.contains("A/S철거|AS철거", na=False)].copy()
                recs, dup_cnt, total_in = [], 0, len(as_in)

                for i, (_, row) in enumerate(as_in.iterrows()):
                    in_date = pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                    code = str(row.iloc[7]).strip().upper()
                    
                    if f"{in_date}|{code}" in existing_combos:
                        dup_cnt += 1; continue
                    
                    m_info = st.session_state.master_lookup.get(sanitize_code(row.iloc[3]), {})
                    recs.append({
                        "압축코드": code, "자재번호": sanitize_code(row.iloc[3]), "자재명": str(row.iloc[4]).strip(),
                        "규격": str(row.iloc[5]).strip(), "공급업체명": m_info.get("업체", "미등록"),
                        "분류구분": m_info.get("분류", "수리대상"), "입고일": in_date, "상태": "출고 대기"
                    })
                    
                    if len(recs) >= 200:
                        supabase.table("as_history").insert(recs).execute()
                        recs = []
                        ui_msg.warning(f"🚀 [2/2] 신규 데이터 저장 중... ({i+1:,} / {total_in:,} 건)")
                        ui_prog.progress(min((i+1)/total_in, 1.0))
                
                if recs: supabase.table("as_history").insert(recs).execute()
                ui_msg.success(f"✅ 입고 완료 (신규: {total_in-dup_cnt:,} / 중복제외: {dup_cnt:,})")
                ui_prog.progress(1.0)
            except Exception as e: st.error(f"입고 오류: {e}")

# --- [TAB 2] 출고 처리 (진행 사항 모니터링) ---
with tab2:
    st.subheader("📤 AS 출고 및 TAT 반영")
    out_file = st.file_uploader("출고 결과 엑셀 업로드", type=['xlsx'], key="final_out_up")
    
    if out_file and st.button("🚀 출고 데이터 반영 시작", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            df_out = pd.read_excel(out_file).fillna("")
            as_out = df_out[df_out.iloc[:, 3].astype(str).str.contains('AS 카톤 박스', na=False)].copy()
            
            ui_msg.info("🔍 [1/3] DB 데이터 로드 중...")
            db_res = supabase.table("as_history").select("id, 압축코드, 입고일, 출고일, 상태").execute()
            
            ui_msg.info("⚙️ [2/3] 고속 매칭 엔진 가동 중...")
            db_lookup = {}
            for r in db_res.data:
                c = str(r['압축코드']).strip().upper()
                if c not in db_lookup: db_lookup[c] = []
                db_lookup[c].append(r)
            
            update_list, total_out = [], len(as_out)
            for i, (_, row) in enumerate(as_out.iterrows()):
                code = str(row.iloc[10]).strip().upper()
                ex_out_date = pd.to_datetime(row.iloc[6]).strftime('%Y-%m-%d')
                
                for db_row in db_lookup.get(code, []):
                    if str(db_row['입고일']) <= ex_out_date:
                        if db_row['상태'] == "출고 완료" and str(db_row['출고일']) == ex_out_date: continue
                        update_list.append({"id": db_row['id'], "출고일": ex_out_date})
                
                if i % 100 == 0:
                    ui_msg.info(f"🧪 유효성 검증 중... ({i+1:,} / {total_out:,})")
                    ui_prog.progress(min((i+1)/total_out, 1.0))

            if update_list:
                for idx, item in enumerate(update_list):
                    supabase.table("as_history").update({"출고일": item['출고일'], "상태": "출고 완료"}).eq("id", item['id']).execute()
                    if idx % 50 == 0:
                        ui_msg.warning(f"🔄 [3/3] DB 최종 반영 중... ({idx:,} / {len(update_list):,})")
                        ui_prog.progress(min(idx/len(update_list), 1.0))
                ui_msg.success(f"✅ 반영 완료: {len(update_list):,}건")
                ui_prog.progress(1.0)
        except Exception as e: st.error(f"출고 오류: {e}")

# --- [TAB 3] 분석 리포트 (진행 사항 모니터링) ---
with tab3:
    st.subheader("📊 TAT 분석 리포트 생성")
    if st.button("📊 전체 리포트 파일 생성", use_container_width=True):
        ui_msg, ui_prog = st.empty(), st.progress(0)
        try:
            all_data, offset = [], 0
            while True:
                res = supabase.table("as_history").select("*").range(offset, offset + 999).execute()
                if not res.data: break
                all_data.extend(res.data)
                offset += len(res.data)
                ui_msg.info(f"📥 DB 데이터 수집 중... ({offset:,} 건)")
                if len(res.data) < 1000: break
            
            if all_data:
                df = pd.DataFrame(all_data)
                df['입고일'] = pd.to_datetime(df['입고일'], errors='coerce')
                df['출고일'] = pd.to_datetime(df['출고일'], errors='coerce')
                df['tat'] = (df['출고일'] - df['입고일']).dt.days
                
                cols = ['입고일', '자재번호', '자재명', '규격', '공급업체명', '분류구분', '압축코드', '출고일', 'tat']
                existing = [c for c in cols if c in df.columns]

                def make_bin(target_df):
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine='xlsxwriter') as wr:
                        if target_df.empty: pd.DataFrame(columns=existing).to_excel(wr, index=False)
                        else: target_df[existing].to_excel(wr, index=False)
                    return out.getvalue()

                st.session_state.bin_tat = make_bin(df[df['출고일'].notna()])
                st.session_state.bin_stay = make_bin(df[df['출고일'].isna()])
                st.session_state.bin_total = make_bin(df)
                st.session_state.data_ready = True
                ui_msg.success("✅ 생성 완료! 아래 다운로드 버튼이 활성화되었습니다.")
                st.rerun()
        except Exception as e: st.error(f"리포트 오류: {e}")

    if st.session_state.get("data_ready"):
        st.divider()
        c1, c2, c3 = st.columns(3)
        with c1: st.download_button("📥 출고완료", st.session_state.bin_tat, "1_done.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        with c2: st.download_button("📥 미출고", st.session_state.bin_stay, "2_pending.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        with c3: st.download_button("📥 전체합계", st.session_state.bin_total, "3_all.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
