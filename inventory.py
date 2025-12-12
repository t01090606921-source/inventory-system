import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io
from supabase import create_client, Client
import math
import uuid

# [안전장치] 라이브러리 체크
try:
    from streamlit_calendar import calendar
except ImportError:
    st.error("🚨 'streamlit-calendar' 라이브러리가 없습니다. requirements.txt를 확인하세요.")
    # 라이브러리 없어도 코드는 돌아가게(리스트형 강제) flag 설정 가능하지만 일단 경고
    
# --- [1] 로그인 보안 ---
def check_password():
    if 'password_correct' not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True
    
    st.set_page_config(page_title="재고관리(하이브리드)", layout="wide")
    st.title("🏭 디지타스 창고 재고관리 (Ver.12.7)")
    pwd = st.text_input("비밀번호를 입력하세요", type="password")
    if st.button("로그인"):
        if pwd == "1234": 
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
    return False

if not check_password():
    st.stop()

# --- [2] Supabase 연결 설정 ---
@st.cache_resource
def init_connection():
    try:
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"❌ Supabase 연결 실패: {e}")
        return None

supabase = init_connection()

# --- [공통] 대용량 데이터 가져오기 ---
def fetch_all_data(table_name, sort_col):
    if not supabase: return []
    all_data = []
    page_size = 1000
    offset = 0
    while True:
        try:
            response = supabase.table(table_name).select("*").order(sort_col).range(offset, offset + page_size - 1).execute()
            data = response.data
            if not data: break
            all_data.extend(data)
            if len(data) < page_size: break
            offset += page_size
        except Exception:
            break
    return all_data

# --- [3-A] 무거운 데이터 로드 ---
@st.cache_data(ttl=21600, show_spinner=False)
def load_heavy_data():
    if not supabase: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        data_m = fetch_all_data("품목표", "품목코드")
        df_m = pd.DataFrame(data_m)
        data_map = fetch_all_data("매핑정보", "box번호")
        df_map = pd.DataFrame(data_map)
        data_d = fetch_all_data("상세내역", "box번호")
        df_d = pd.DataFrame(data_d) 
        for df in [df_m, df_map, df_d]:
            if not df.empty: df.columns = [c.lower() for c in df.columns]
        return df_m, df_map, df_d
    except: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# --- [3-B] 가벼운 데이터 로드 ---
@st.cache_data(ttl=600, show_spinner=False)
def load_light_data():
    if not supabase: return pd.DataFrame()
    try:
        data_l = fetch_all_data("입출고", "id")
        df_l = pd.DataFrame(data_l)
        if not df_l.empty: df_l.columns = [c.lower() for c in df_l.columns]
        return df_l
    except: return pd.DataFrame()

def clear_cache_all():
    st.cache_data.clear()

# --- [4] 재고 현황 계산 ---
@st.cache_data(show_spinner=False)
def calculate_stock_snapshot(df_log, df_mapping, df_master, df_details):
    if df_log.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    last_stat = df_log.sort_values('id').groupby('box번호').tail(1)
    stock_boxes = last_stat[last_stat['구분'].isin(['입고', '이동'])].copy()
    if stock_boxes.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    if df_mapping.empty: df_mapping = pd.DataFrame(columns=['match_key', 'box번호', '품목코드', '수량'])
    else:
        if 'box번호' in df_mapping.columns: df_mapping['match_key'] = df_mapping['box번호'].astype(str).str.strip().str.upper()
        else: df_mapping['match_key'] = ""

    stock_boxes['match_key'] = stock_boxes['box번호'].astype(str).str.strip().str.upper()
    merged = pd.merge(stock_boxes, df_mapping, on='match_key', how='left', suffixes=('', '_map'))
    merged['위치'] = merged['위치'].fillna('미지정')
    
    if not df_master.empty and '품목코드' in df_master.columns:
        df_master['품목코드'] = df_master['품목코드'].astype(str).str.strip().str.upper()
        if '품목코드' in merged.columns:
            merged = pd.merge(merged, df_master, on='품목코드', how='left')

    filtered_details = pd.DataFrame()
    if not df_details.empty and 'box번호' in df_details.columns:
        df_details['match_key'] = df_details['box번호'].astype(str).str.strip().str.upper()
        active = stock_boxes['match_key'].unique()
        filtered_details = df_details[df_details['match_key'].isin(active)].copy()
        
    return stock_boxes, merged, filtered_details

# --- 데이터 업로드 ---
def chunked_insert(table_name, df):
    if not supabase or df.empty: return False
    try:
        df = df.where(pd.notnull(df), None)
        total = len(df)
        batch = 5000
        chunks = math.ceil(total / batch)
        bar = st.progress(0, text=f"{table_name} 업로드...")
        for i in range(chunks):
            start = i * batch
            end = start + batch
            chunk = df.iloc[start:end]
            data = chunk.to_dict(orient='records')
            supabase.table(table_name).insert(data).execute()
            bar.progress(min((i+1)/chunks, 1.0))
        bar.empty()
        return True
    except Exception as e:
        st.error(f"실패: {e}")
        return False

# --- 일정 관리 (Native & Calendar) ---
def fetch_schedules_native():
    if not supabase: return []
    try:
        res = supabase.table("schedule").select("*").order("start_time", desc=True).execute()
        return res.data
    except: return []

def add_schedule(title, start_time):
    if not supabase: return
    try:
        supabase.table("schedule").insert({"title": title, "start_time": start_time}).execute()
        st.session_state.calendar_key = str(uuid.uuid4()) # 캘린더 갱신 트리거
        return True
    except: return False

def delete_schedule(id):
    if not supabase: return
    try:
        supabase.table("schedule").delete().eq("id", id).execute()
        st.session_state.calendar_key = str(uuid.uuid4()) # 캘린더 갱신 트리거
        return True
    except: return False

# --- 유틸리티 ---
def init_session_state():
    if 'scan_buffer' not in st.session_state: st.session_state.scan_buffer = []
    if 'proc_msg' not in st.session_state: st.session_state.proc_msg = None
    if 'selected_rack' not in st.session_state: st.session_state.selected_rack = None
    if 'calendar_key' not in st.session_state: st.session_state.calendar_key = str(uuid.uuid4())
    if 'view_mode' not in st.session_state: st.session_state.view_mode = "Calendar" # 기본값 달력

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def get_sample_file():
    sample_data = {'날짜': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],'이동구분': ['입고'],'입고구분': ['일반철거'],'Box번호': ['V2024...'],'위치': ['1-2-7'],'파렛트': ['P-01']}
    return to_excel(pd.DataFrame(sample_data))

def buffer_scan(df_master, df_mapping, df_log, df_details):
    scan_val = str(st.session_state.scan_input).strip().upper()
    mode = st.session_state.work_mode
    curr_loc = str(st.session_state.get('curr_location', '')).strip()
    curr_pal = str(st.session_state.get('curr_palette', '')).strip()
    if not scan_val: return

    disp_name, disp_spec, disp_qty, p_code = "정보없음", "규격없음", 0, ""
    if not df_mapping.empty and 'box번호' in df_mapping.columns:
        df_mapping['temp_key'] = df_mapping['box번호'].astype(str).str.strip().str.upper()
        map_info = df_mapping[df_mapping['temp_key'] == scan_val]
        if not map_info.empty:
            p_code = str(map_info.iloc[0]['품목코드']).strip()
            disp_qty = map_info.iloc[0]['수량']
            if not df_master.empty and '품목코드' in df_master.columns:
                df_master['temp_key'] = df_master['품목코드'].astype(str).str.strip().str.upper()
                m_info = df_master[df_master['temp_key'] == p_code.upper()]
                if not m_info.empty:
                    disp_name = m_info.iloc[0]['품명']
                    disp_spec = m_info.iloc[0]['규격']

    is_compressed = False
    target_box_no = scan_val
    if p_code == "정보없음":
        if not df_details.empty and '압축코드' in df_details.columns:
            df_details['temp_code'] = df_details['압축코드'].astype(str).str.strip().str.upper()
            matched = df_details[df_details['temp_code'] == scan_val]
            if not matched.empty:
                target_box_no = str(matched.iloc[0]['box번호']).strip().upper()
                is_compressed = True
                if not df_mapping.empty:
                    df_mapping['temp_key'] = df_mapping['box번호'].astype(str).str.strip().str.upper()
                    map_info = df_mapping[df_mapping['temp_key'] == target_box_no]
                    if not map_info.empty:
                        p_code = str(map_info.iloc[0]['품목코드']).strip()
                        disp_qty = map_info.iloc[0]['수량']
                        if not df_master.empty:
                            m_info = df_master[df_master['temp_key'] == p_code.upper()]
                            if not m_info.empty:
                                disp_name = m_info.iloc[0]['품명']
                                disp_spec = m_info.iloc[0]['규격']

    box_status, current_db_loc = "신규", "미지정"
    if not df_log.empty and 'box번호' in df_log.columns:
        df_log['temp_key'] = df_log['box번호'].astype(str).str.strip().str.upper()
        my_logs = df_log[df_log['temp_key'] == target_box_no]
        if not my_logs.empty:
            last_log = my_logs.iloc[0]
            last_action = last_log['구분']
            current_db_loc = last_log['위치']
            if last_action in ['입고', '이동']: box_status = f"창고있음({current_db_loc})"
            elif last_action == '출고': box_status = "출고됨"

    is_duplicate = (mode == "입고" and "창고있음" in box_status)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg_prefix = "📦 압축코드 인식 → " if is_compressed else ""
    
    if mode == "조회(검색)":
        msg_text = f"🔎 {msg_prefix}Box: {target_box_no} / {disp_name} / {disp_spec} / {disp_qty}개 / {current_db_loc}"
        st.session_state.proc_msg = ("info", msg_text)
    elif mode == "출고":
        if "창고있음" not in box_status:
            st.session_state.proc_msg = ("error", f"⛔ 출고 불가: Box [{target_box_no}] 재고 없음")
        else:
            log_entry = {'날짜': now_str, '구분': mode, '입고구분': '', 'Box번호': target_box_no, '품목코드': p_code, '규격': disp_spec, '수량': disp_qty, '위치': final_loc, '파렛트': final_pal}
            st.session_state.scan_buffer.append(log_entry)
            st.session_state.proc_msg = ("success", f"✅ {msg_prefix}출고 대기: {target_box_no}")
    else: 
        if is_duplicate:
            st.session_state.proc_msg = ("error", f"⛔ 이미 입고됨: {target_box_no}")
        else:
            final_loc = curr_loc if curr_loc else "미지정"
            final_pal = curr_pal if curr_pal else "이름없음"
            log_entry = {'날짜': now_str, '구분': mode, '입고구분': '', 'Box번호': target_box_no, '품목코드': p_code, '규격': disp_spec, '수량': disp_qty, '위치': final_loc, '파렛트': final_pal}
            st.session_state.scan_buffer.append(log_entry)
            st.session_state.proc_msg = ("success", f"✅ {msg_prefix}{mode}: {target_box_no}")
            
    st.session_state.scan_input = ""

# --- 캘린더 다이얼로그 (Dialog) ---
@st.dialog("일정 관리")
def schedule_dialog(sel_date=None, event_data=None):
    if event_data: # 수정/삭제
        st.subheader("일정 수정/삭제")
        new_title = st.text_input("업체명 / 내용", value=event_data["title"])
        
        # 날짜 파싱 안전장치
        try:
            dt_obj = datetime.fromisoformat(event_data["start"])
            d_val = dt_obj.date()
            t_val = dt_obj.time()
        except:
            # 파싱 실패 시 현재 시간으로 대체 (에러 방지)
            d_val = datetime.now().date()
            t_val = datetime.now().time()
            
        new_date = st.date_input("날짜", value=d_val)
        new_time = st.time_input("시간", value=t_val)
        
        c1, c2 = st.columns(2)
        if c1.button("수정 저장", type="primary"):
            final_dt = datetime.combine(new_date, new_time).isoformat()
            if update_schedule(event_data["id"], new_title, final_dt):
                st.success("수정되었습니다."); st.rerun()
        if c2.button("삭제", type="secondary"):
            if delete_schedule(event_data["id"]):
                st.success("삭제되었습니다."); st.rerun()
    else: # 신규
        st.subheader("새 일정 등록")
        st.write(f"선택된 날짜: {sel_date}")
        title = st.text_input("업체명 / 내용")
        time_val = st.time_input("시간", value=datetime.now().time())
        if st.button("등록"):
            if title:
                final_dt = f"{sel_date}T{time_val}"
                if add_schedule(title, final_dt):
                    st.success("등록되었습니다."); st.rerun()
            else: st.warning("내용을 입력하세요.")

def main():
    if 'scan_buffer' not in st.session_state: st.session_state.scan_buffer = []
    if 'proc_msg' not in st.session_state: st.session_state.proc_msg = None
    if 'selected_rack' not in st.session_state: st.session_state.selected_rack = None

    with st.spinner("📦 데이터 로드 중..."):
        df_master, df_mapping, df_details = load_heavy_data()
        df_log = load_light_data()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["1. 연속 스캔", "2. 재고 현황", "3. 일괄 업로드", "4. 포장데이터", "5. 품목 마스터", "6. 데이터 진단", "7. 월간 일정"])

    with tab1:
        st.subheader("🚀 스캔 작업")
        if st.button("🔄 새로고침", key='r1'): clear_cache_all(); st.rerun()
        
        if st.session_state.proc_msg:
            m_type, m_text = st.session_state.proc_msg
            if m_type == 'success': st.success(m_text)
            elif m_type == 'error': st.error(m_text)
            else: st.info(m_text)

        c1, c2, c3, c4 = st.columns([1.5, 1, 1, 2])
        with c1: st.radio("모드", ["입고", "재고이동", "출고", "조회(검색)"], horizontal=True, key="work_mode")
        with c2: st.text_input("적재 위치 (1-2-7)", key="curr_location")
        with c3: st.text_input("파렛트 이름", key="curr_palette")
        with c4: st.text_input("Box 번호 또는 압축코드 스캔", key="scan_input", on_change=buffer_scan, args=(df_master, df_mapping, df_log, df_details))

        if st.session_state.scan_buffer:
            disp_df = pd.DataFrame(st.session_state.scan_buffer)
            cols_order = ['날짜', '구분', '입고구분', 'Box번호', '품목코드', '규격', '수량', '위치', '파렛트']
            final_cols = [c for c in cols_order if c in disp_df.columns]
            st.dataframe(disp_df[final_cols].iloc[::-1], use_container_width=True)
            
            csv_data = to_excel(disp_df[final_cols])
            st.download_button("📥 스캔 목록 다운로드", data=csv_data, file_name=f"스캔목록_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else: st.info("대기 중...")
        
        if st.button("💾 DB에 저장 (빠름)", type="primary", use_container_width=True): 
            if insert_log(st.session_state.scan_buffer):
                st.session_state.scan_buffer = []
                st.session_state.proc_msg = ("success", "✅ 저장 완료!")
                st.rerun()
        if st.button("🗑️ 대기 목록 비우기", use_container_width=True): st.session_state.scan_buffer = []

        st.divider()
        st.subheader("📊 최근 입출고 이력 (전체)")
        if not df_log.empty:
            csv_data = to_excel(df_log)
            st.download_button("📥 전체 입출고 이력 다운로드", data=csv_data, file_name=f"전체이력_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.dataframe(df_log.head(1000), use_container_width=True)
        else: st.info("이력이 없습니다.")

    with tab2:
        view_inventory_dashboard(df_log, df_mapping, df_master, df_details)

    with tab3:
        st.subheader("📤 입출고 내역 일괄 업로드")
        st.download_button("📥 샘플 양식 다운로드", get_sample_file(), "입출고_샘플.xlsx")
        st.info("양식: 날짜 / 이동구분 / 입고구분 / Box번호 / 위치 / 파렛트")
        
        up = st.file_uploader("엑셀 파일", type=['xlsx', 'csv'])
        if up and st.button("DB 업로드 (대용량 대응)"):
            try:
                df = pd.read_excel(up) if up.name.endswith('xlsx') else pd.read_csv(up)
                clean_df = pd.DataFrame()
                
                df.columns = df.columns.str.strip().str.replace(' ', '')
                col_box = next((c for c in df.columns if 'box' in c.lower() or '박스' in c), None)
                if not col_box: st.error("❌ 'Box번호' 컬럼이 없습니다."); st.stop()
                
                col_gubun = next((c for c in df.columns if ('이동구분' in c) or ('구분' in c and '입고' not in c)), None)
                col_in_type = next((c for c in df.columns if '입고구분' in c), None)
                col_loc = next((c for c in df.columns if '위치' in c), None)
                col_pal = next((c for c in df.columns if '파렛트' in c or '팔레트' in c), None)
                col_date = next((c for c in df.columns if '날짜' in c), None)

                if col_date: clean_df['날짜'] = df[col_date].astype(str).replace('nan', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                else: clean_df['날짜'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                clean_df['구분'] = df[col_gubun].astype(str) if col_gubun else '입고'
                clean_df['입고구분'] = df[col_in_type].astype(str).replace('nan', '') if col_in_type else ''
                clean_df['box번호'] = df[col_box].astype(str).str.strip().str.upper()
                clean_df['위치'] = df[col_loc].astype(str).replace('nan', '') if col_loc else ''
                clean_df['파렛트'] = df[col_pal].astype(str).replace('nan', '') if col_pal else ''

                current_stock, _, _ = calculate_stock_snapshot(df_log, df_mapping, df_master, df_details)
                available_boxes = set(current_stock['match_key'].values) if not current_stock.empty else set()
                outbound_check = clean_df[clean_df['구분'] == '출고']
                missing_boxes = [b for b in outbound_check['box번호'] if b not in available_boxes]
                
                if missing_boxes:
                    st.error(f"⛔ 업로드 불가: 재고 없음 - {missing_boxes[:5]}...")
                    st.stop()

                if chunked_insert('입출고', clean_df):
                    st.success("✅ 완료!")
                    clear_cache_all()
                    st.rerun()
            except Exception as e:
                st.error(f"오류: {e}")

    with tab4:
        st.subheader("📦 포장데이터 등록")
        up_pack = st.file_uploader("포장 파일 (.xlsx)", type=['xlsx'])
        if up_pack and st.button("등록 (대용량)"):
            try:
                raw = pd.read_excel(up_pack, dtype=str)
                raw = raw.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                
                grp = raw.groupby(['카톤박스번호', '박스자재코드']).size().reset_index(name='수량')
                grp.columns = ['box번호', '품목코드', '수량']
                grp['box번호'] = grp['box번호'].str.upper()
                
                dets = pd.DataFrame(columns=['box번호', '품목코드', '규격', '압축코드'])
                if '압축코드' in raw.columns:
                    dets = raw[['카톤박스번호', '박스자재코드', '박스자재규격', '압축코드']].copy()
                    dets.columns = ['box번호', '품목코드', '규격', '압축코드']
                    dets['box번호'] = dets['box번호'].str.upper()

                items = raw[['박스자재코드', '박스자재명', '박스자재규격', '출고처명']].drop_duplicates('박스자재코드')
                items.columns = ['품목코드', '품명', '규격', '공급업체']
                items['품목코드'] = items['품목코드'].str.upper()
                items['분류구분'] = ''
                items['바코드'] = ''

                st.write("품목표 업로드...")
                chunked_upsert('품목표', items, '품목코드')
                st.write("매핑정보 업로드...")
                chunked_upsert('매핑정보', grp, 'box번호')
                if not dets.empty:
                    st.write("상세내역 업로드...")
                    chunked_insert('상세내역', dets)
                
                clear_cache_all()
                st.success("완료!")
                st.rerun()
            except Exception as e: st.error(f"오류: {e}")

    with tab5:
        st.dataframe(df_master.head(1000))

    with tab6:
        st.subheader("🕵️‍♀️ 데이터 진단 (총량 확인)")
        if st.button("🔄 [필수] 캐시 삭제 및 데이터 재로드", type="primary"):
            clear_cache_all()
            st.rerun()
        c1, c2, c3 = st.columns(3)
        c1.metric("품목표", f"{len(df_master)}건")
        c2.metric("매핑정보", f"{len(df_mapping)}건")
        c3.metric("입출고", f"{len(df_log)}건")
        st.write("▼ 매핑정보 샘플")
        st.dataframe(df_mapping.head(50))

    with tab7:
        st.subheader("🗓️ 월간 출고 일정")
        
        # [하이브리드 모드] 보기 모드 선택
        view_mode = st.radio("보기 모드", ["Calendar (달력)", "List (리스트)"], horizontal=True, key='view_mode_radio')
        
        # 전체 일정 데이터 로드 (Native 방식)
        raw_schedules = fetch_schedules_native()
        
        if view_mode == "List (리스트)":
            # --- 리스트형 뷰 (안전) ---
            c1, c2 = st.columns([1, 2])
            with c1:
                st.markdown("##### ✏️ 일정 등록")
                sel_date = st.date_input("날짜", value=datetime.now())
                evt_title = st.text_input("내용")
                evt_time = st.time_input("시간", value=datetime.now().time())
                if st.button("추가", type="primary"):
                    final_dt = datetime.combine(sel_date, evt_time).isoformat()
                    if add_schedule(evt_title, final_dt):
                        st.success("등록됨")
                        st.rerun()
            with c2:
                st.markdown(f"##### 📋 {sel_date} 일정")
                # 해당 날짜 필터링 (안전한 날짜 변환 사용)
                day_evts = []
                for s in raw_schedules:
                    try:
                        s_dt = pd.to_datetime(s['start_time']).date()
                        if s_dt == sel_date: day_evts.append(s)
                    except: continue
                
                if day_evts:
                    for e in day_evts:
                        with st.expander(f"{e['title']}"):
                            if st.button("삭제", key=f"del_l_{e['id']}"):
                                delete_schedule(e['id'])
                                st.rerun()
                else: st.info("일정 없음")
                
                st.divider()
                st.markdown("##### 📅 전체 목록")
                if raw_schedules:
                    df_s = pd.DataFrame(raw_schedules)
                    df_s['시간'] = pd.to_datetime(df_s['start_time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
                    st.dataframe(df_s[['시간', 'title']], use_container_width=True)

        else:
            # --- 달력형 뷰 (도전) ---
            try:
                from streamlit_calendar import calendar
                # 데이터 포맷팅 (캘린더 라이브러리용)
                cal_events = []
                for s in raw_schedules:
                    try:
                        # 날짜 유효성 체크
                        pd.to_datetime(s['start_time']) 
                        cal_events.append({
                            "id": str(s['id']),
                            "title": s['title'],
                            "start": s['start_time'],
                            "allDay": False
                        })
                    except: continue

                cal_key = f"cal_{st.session_state.calendar_key}"
                cal = calendar(
                    events=cal_events,
                    options={
                        "headerToolbar": {"left": "today prev,next", "center": "title", "right": "dayGridMonth,timeGridWeek,timeGridDay"},
                        "initialView": "dayGridMonth",
                    },
                    custom_css={'height': '600px'},
                    key=cal_key
                )
                
                if cal.get("callback") == "dateClick":
                    schedule_dialog(sel_date=cal["dateClick"]["date"])
                elif cal.get("callback") == "eventClick":
                    evt_id = cal["eventClick"]["event"]["id"]
                    evt_data = next((e for e in cal_events if e["id"] == evt_id), None)
                    if evt_data: schedule_dialog(event_data=evt_data)
                    
            except Exception as e:
                st.error(f"캘린더 로드 오류: {e}")
                st.info("👆 위쪽 '보기 모드'를 [List (리스트)]로 변경하여 사용하세요.")

if __name__ == '__main__':
    main()
