import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io
import time

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("🚀 AS TAT 분석 시스템 (57만 건 대응)")

# --- 2. 사이드바: 마스터 및 관리 (기능 복구) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [전수 로드 함수]
    def fetch_all_data(table_name):
        all_data = []
        limit = 1000
        offset = 0
        status_area = st.empty()
        while True:
            res = supabase.table(table_name).select("*").range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
            status_area.text(f"데이터 로드 중: {offset:,} 건...")
        status_area.empty()
        return pd.DataFrame(all_data)

    # --- 마스터 관리 기능 (복구 완료) ---
    st.subheader("1. 마스터 데이터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'], key="master_up")
    
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        with st.spinner("마스터 데이터를 정리 중..."):
            m_df_raw = pd.read_excel(master_file, dtype=str)
            t_col = next((c for c in m_df_raw.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df_raw.columns[0])
            
            m_data = []
            for _, row in m_df_raw.iterrows():
                mat_val = str(row[t_col]).strip().upper()
                if not mat_val or mat_val == "NAN": continue
                m_data.append({
                    "자재번호": mat_val,
                    "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                    "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"
                })
            
            if m_data:
                # 기존 마스터 삭제 후 재등록
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success(f"✅ 마스터 {len(m_data):,}건 등록 완료")
                st.rerun()

    # 현재 마스터 정보 표시
    try:
        m_df_local = fetch_all_data("master_data")
        st.info(f"📊 현재 마스터 DB: {len(m_df_local):,} 건")
    except:
        m_df_local = pd.DataFrame()

    st.divider()
    if st.button("⚠️ AS 데이터 전체 초기화", type="primary", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.warning("입/출고 히스토리가 초기화되었습니다.")
        st.rerun()

# --- 3. 입고/출고 처리 (57만 건 대응) ---
tab1, tab2 = st.tabs(["📥 대량 입고 (수신)", "📤 대량 출고 (송신)"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 (최대 60만 건 가능)", type=['xlsx'], key="in_bulk")
    if in_file and st.button("🚀 입고 및 전수 매칭 시작"):
        if m_df_local.empty:
            st.error("사이드바에서 마스터 데이터를 먼저 등록해 주세요.")
        else:
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            
            total_rows = len(as_in)
            status_text = st.empty()
            prog_bar = st.progress(0)
            
            recs = []
            for i, (_, row) in enumerate(as_in.iterrows()):
                mat_val = str(row.iloc[3]).strip().upper()
                m_info = m_lookup.get(mat_val)
                
                recs.append({
                    "압축코드": str(row.iloc[7]).strip(), "자재번호": mat_val,
                    "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                    "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                    "분류구분": m_info['분류구분'] if m_info else "미등록",
                    "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
                })
                
                # 1,000건 단위 분할 업로드
                if len(recs) == 1000:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    prog_bar.progress((i + 1) / total_rows)
                    status_text.text(f"처리 중: {i+1:,} / {total_rows:,} 건 완료")
            
            if recs:
                supabase.table("as_history").insert(recs).execute()
            
            st.success(f"✅ 총 {total_rows:,} 건 입고 및 매칭 완료!")
            st.rerun()

with tab2:
    out_file = st.file_uploader("출고 엑셀 업로드", type=['xlsx'], key="out_bulk")
    if out_file and st.button("🚀 출고 대량 처리"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            
            # 500건씩 분할 업데이트
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"})\
                        .in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 매칭 완료")
            st.rerun()

# --- 4. 리포트 영역 ---
st.divider()
if st.button("📊 전수 데이터 불러오기 (50만 건 이상일 때 클릭)"):
    df_res = fetch_all_data("as_history")
    if not df_res.empty:
        st.subheader("📊 TAT 분석 리포트")
        
        # 필터 3종
        c1, c2, c3 = st.columns(3)
        v_f = c1.multiselect("🏢 공급업체", sorted(df_res['공급업체명'].unique()))
        g_f = c2.multiselect("📂 분류구분", sorted(df_res['분류구분'].unique()))
        s_f = c3.multiselect("🚚 상태", sorted(df_res['상태'].unique()))
        
        dff = df_res.copy()
        if v_f: dff = dff[dff['공급업체명'].isin(v_f)]
        if g_f: dff = dff[dff['분류구분'].isin(g_f)]
        if s_f: dff = dff[dff['상태'].isin(s_f)]

        # 요약 지표
        m1, m2, m3 = st.columns(3)
        m1.metric("전체 건수", f"{len(dff):,} 건")
        m2.metric("미등록 건수", f"{len(dff[dff['공급업체명'] == '미등록']):,} 건")
        m3.metric("출고 대기", f"{len(dff[dff['상태'] == '출고 대기']):,} 건")

        # 엑셀 다운로드
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            dff.to_excel(writer, index=False)
        st.download_button("📥 엑셀 결과 다운로드", output.getvalue(), "AS_TAT_Report.xlsx")

        # 화면 표시 (성능을 위해 1만건 제한)
        if len(dff) > 10000:
            st.warning("화면에는 상위 10,000건만 표시됩니다. 전체 데이터는 엑셀을 다운로드하세요.")
            st.dataframe(dff.head(10000), use_container_width=True, hide_index=True)
        else:
            st.dataframe(dff, use_container_width=True, hide_index=True)
