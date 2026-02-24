import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 시스템", layout="wide")
st.title("📊 AS TAT 분석 및 관리 시스템")

# --- 2. 사이드바: 마스터 및 데이터 전수 관리 (복구 완료) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [전수 로드 함수]
    def fetch_all_data(table_name, columns="*"):
        all_data = []
        limit = 1000
        offset = 0
        status_area = st.empty()
        while True:
            res = supabase.table(table_name).select(columns).range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
            status_area.text(f"데이터 로드 중: {offset:,} 건...")
        status_area.empty()
        return pd.DataFrame(all_data)

    # --- 🔥 마스터 업로드 기능 (복구) ---
    st.subheader("1. 마스터 엑셀 등록")
    master_file = st.file_uploader("마스터 파일 선택", type=['xlsx'], key="m_up")
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        with st.spinner("마스터 데이터 동기화 중..."):
            m_df_raw = pd.read_excel(master_file, dtype=str)
            t_col = next((c for c in m_df_raw.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df_raw.columns[0])
            m_data = [{"자재번호": str(row[t_col]).strip().upper(), 
                       "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                       "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                      for _, row in m_df_raw.iterrows() if not pd.isna(row[t_col])]
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success("✅ 마스터 등록 완료")
                st.rerun()

    # 현재 마스터 수량 표시
    try:
        m_df_local = fetch_all_data("master_data")
        st.info(f"📊 현재 마스터 DB: {len(m_df_local):,} 건")
    except:
        m_df_local = pd.DataFrame()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.success("히스토리 초기화 완료")
        st.rerun()

# --- 3. 입고/출고 처리 (57만 건 분할 처리) ---
tab1, tab2 = st.tabs(["📥 AS 입고", "📤 AS 출고"])

with tab1:
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_bulk")
    if in_file and st.button("입고 및 매칭 실행"):
        if m_df_local.empty:
            st.error("사이드바에서 마스터 데이터를 먼저 등록해 주세요.")
        else:
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            
            total_rows, recs = len(as_in), []
            prog_bar, status_text = st.progress(0), st.empty()
            
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
                if len(recs) == 1000:
                    supabase.table("as_history").insert(recs).execute()
                    recs = []
                    prog_bar.progress((i + 1) / total_rows)
                    status_text.text(f"처리 중: {i+1:,} / {total_rows:,} 건 완료")
            if recs: supabase.table("as_history").insert(recs).execute()
            st.success("✅ 57만 건 전수 입고 완료")

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_bulk")
    if out_file and st.button("출고 대량 매칭"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"})\
                        .in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 처리 완료")

# --- 4. [경량화] TAT 분석 리포트 ---
st.divider()
st.subheader("📈 수리대상 TAT 통계 분석")
if st.button("🔍 리포트 생성 (가벼운 버전)", use_container_width=True):
    with st.spinner("분석용 데이터 로드 중..."):
        df_raw = fetch_all_data("as_history", "입고일, 출고일, 공급업체명, 분류구분")
    
    if not df_raw.empty:
        df_raw['입고일'] = pd.to_datetime(df_raw['입고일'], errors='coerce')
        df_raw['출고일'] = pd.to_datetime(df_raw['출고일'], errors='coerce')
        
        # 수리대상 필터링 및 TAT 계산
        df_repair = df_raw[df_raw['분류구분'] == '수리대상'].copy()
        df_repair['TAT'] = (df_repair['출고일'] - df_repair['입고일']).dt.days
        
        # 업체별 요약 통계
        summary = df_repair[df_repair['출고일'].notna()].groupby('공급업체명').agg(
            수리완료=('TAT', 'count'),
            평균TAT=('TAT', 'mean')
        ).reset_index()
        summary['평균TAT'] = summary['평균TAT'].round(1)
        
        # 메트릭 표시
        m1, m2 = st.columns(2)
        m1.metric("전체 수리대상", f"{len(df_repair):,} 건")
        m2.metric("전체 평균 TAT", f"{df_repair['TAT'].mean():.1f} 일")

        # 결과 테이블
        st.write("### 🏢 업체별 TAT 성적표")
        st.dataframe(summary.sort_values('평균TAT'), use_container_width=True, hide_index=True)
        
        # 다운로드 기능
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_repair.to_excel(writer, index=False)
        st.download_button("📥 수리대상 상세 데이터 다운로드", output.getvalue(), "Repair_TAT.xlsx")
    else:
        st.info("조회할 데이터가 없습니다.")
