import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io
from supabase import create_client, Client
import math

# --- [1] 로그인 보안 ---
def check_password():
    if 'password_correct' not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True
    
    st.set_page_config(page_title="재고관리(진단)", layout="wide")
    st.title("🏭 디지타스 창고 재고관리 (Ver.11.2 - 진단모드)")
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

# --- [강제 초기화 버튼] ---
if st.button("🔄 캐시 데이터 강제 삭제 및 새로고침 (클릭)", type="primary", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# --- [핵심] 대용량 데이터 가져오기 (에러 진단 포함) ---
def fetch_all_data(table_name):
    if not supabase: return []
    all_data = []
    page_size = 5000 # 한 번에 5000개 요청
    offset = 0
    
    while True:
        try:
            # 5000개씩 끊어서 요청
            response = supabase.table(table_name).select("*").range(offset, offset + page_size - 1).execute()
            data = response.data
            
            if not data:
                break
                
            all_data.extend(data)
            
            # 가져온 개수가 요청보다 적으면 마지막 페이지임
            if len(data) < page_size:
                break
                
            offset += page_size
        except Exception as e:
            # [진단] 에러 발생 시 멈추지 말고 에러 메시지 출력
            st.error(f"⚠️ {table_name} 데이터 로드 중 오류 발생 (offset: {offset}): {e}")
            break
            
    return all_data

# --- [3] 데이터 로드 (캐싱) ---
@st.cache_data(ttl=3600)
def load_data_from_db():
    if not supabase: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        # 진행 상황 표시
        with st.spinner("대용량 데이터를 불러오는 중입니다... (잠시만 기다려주세요)"):
            data_m = fetch_all_data("품목표")
            df_m = pd.DataFrame(data_m)
            
            data_map = fetch_all_data("매핑정보")
            df_map = pd.DataFrame(data_map)
            
            data_l = fetch_all_data("입출고")
            df_l = pd.DataFrame(data_l)
            
            data_d = fetch_all_data("상세내역") 
            df_d = pd.DataFrame(data_d) 

        for df in [df_m, df_map, df_l, df_d]:
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]

        return df_m, df_map, df_l, df_d
    except Exception as e:
        st.error(f"데이터 로드 전체 실패: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def clear_cache():
    st.cache_data.clear()

# --- [4] 재고 현황 계산 ---
@st.cache_data(show_spinner=False)
def calculate_stock_snapshot(df_log, df_mapping, df_master, df_details):
    if df_log.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    last_stat = df_log.sort_values('id').groupby('box번호').tail(1)
    stock_boxes = last_stat[last_stat['구분'].isin(['입고', '이동'])].copy()
    
    if not stock_boxes.empty:
        stock_boxes['match_key'] = stock_boxes['box번호'].astype(str).str.strip().str.upper()
    
    if not df_mapping.empty:
        df_mapping['match_key'] = df_mapping['box번호'].astype(str).str.strip().str.upper()
        if '품목코드' in df_mapping.columns:
            df_mapping['품목코드'] = df_mapping['품목코드'].astype(str).str.strip().str.upper()

    if not df_master.empty and '품목코드' in df_master.columns:
        df_master['품목코드'] = df_master['품목코드'].astype(str).str.strip().str.upper()

    merged = pd.merge(stock_boxes, df_mapping, on='match_key', how='left', suffixes=('', '_map'))
    merged['위치'] = merged['위치'].fillna('미지정').replace('', '미지정')
    merged['파렛트'] = merged['파렛트'].fillna('이름없음').replace('', '이름없음')
    
    if not df_master.empty and '품목코드' in merged.columns:
        merged = pd.merge(merged, df_master, on='품목코드', how='left')

    filtered_details = pd.DataFrame()
    if not df_details.empty:
        df_details['match_key'] = df_details['box번호'].astype(str).str.strip().str.upper()
        active_keys = stock_boxes['match_key'].unique()
        filtered_details = df_details[df_details['match_key'].isin(active_keys)].copy()
        
        loc_info = stock_boxes[['match_key', '위치', '파렛트']]
        filtered_details = pd.merge(filtered_details, loc_info, on='match_key', how='left')
        
        if 'match_key' in filtered_details.columns: del filtered_details['match_key']
            
    return stock_boxes, merged, filtered_details

# --- 데이터 업로드 ---
def chunked_upsert(table_name, df, key_col, batch_size=5000):
    if not supabase: return False
    if df.empty: return False
    try:
        df = df.astype(str)
        if key_col in df.columns: df[key_col] = df[key_col].str.strip().str.upper()
        df = df.where(pd.notnull(df), None)
        total_rows = len(df)
        chunks = math.ceil(total_rows / batch_size)
        my_bar = st.progress(0, text=f"{table_name} 업로드...")
        for i in range(chunks):
            start = i * batch_size
            end = start + batch_size
            chunk = df.iloc[start:end]
            data = chunk.to_dict(orient='records')
            supabase.table(table_name).upsert(data, on_conflict=key_col).execute()
            my_bar.progress(min((i+1)/chunks, 1.0))
        my_bar.empty()
        return True
    except Exception as e:
        st.error(f"실패: {e}")
        return False

def chunked_insert(table_name, df, batch_size=5000):
    if not supabase: return False
    if df.empty: return False
    try:
        df = df.where(pd.notnull(df), None)
        total_rows = len(df)
        chunks = math.ceil(total_rows / batch_size)
        my_bar = st.progress(0, text=f"{table_name} 추가...")
        for i in range(chunks):
            start = i * batch_size
            end = start + batch_size
            chunk = df.iloc[start:end]
            data = chunk.to_dict(orient='records')
            supabase.table(table_name).insert(data).execute()
            my_bar.progress(min((i+1)/chunks, 1.0))
        my_bar.empty()
        return True
    except Exception as e:
        st.error(f"실패: {e}")
        return False

def insert_log(new_data_list):
    if not supabase: return False
    try:
        cleaned_list = []
        for item in new_data_list:
            cleaned_list.append({
                "날짜": item.get("날짜"),
                "구분": item.get("구분"),
                "입고구분": item.get("입고구분", ""),
                "box번호": str(item.get("Box번호")).strip().upper(), 
                "위치": item.get("위치", ""),
                "파렛트": item.get("파렛트", "")
            })
        supabase.table("입출고").insert(cleaned_list).execute()
        clear_cache()
        return True
    except Exception as e:
        st.error(f"실패: {e}")
        return False

# --- 일정 관리 ---
def fetch_schedules():
    if not supabase: return []
    try:
        res = supabase.table("schedule").select("*").execute()
        events = []
        for item in res.data:
            events.append({
                "id": str(item["id"]),
                "title": item["title"],
                "start": item["start_time"],
                "end": item.get("end_time", ""),
                "allDay": False
            })
        return events
    except Exception as e:
        st.error(f"일정 로드 실패: {e}")
        return []

def add_schedule(title, start_time):
    if not supabase: return
    try:
        supabase.table("schedule").insert({"title": title, "start_time": start_time}).execute()
        return True
    except Exception as e:
        st.error(f"추가 실패: {e}")
        return False

def update_schedule(id, title, start_time):
    if not supabase: return
    try:
        supabase.table("schedule").update({"title": title, "start_time": start_time}).eq("id", id).execute()
        return True
    except Exception as e:
        st.error(f"수정 실패: {e}")
        return False

def delete_schedule(id):
    if not supabase: return
    try:
        supabase.table("schedule").delete().eq("id", id).execute()
        return True
    except Exception as e:
        st.error(f"삭제 실패: {e}")
        return False

# --- 유틸리티 ---
def init_session_state():
    if 'scan_buffer' not in st.session_state: st.session_state.scan_buffer = []
    if 'proc_msg' not in st.session_state: st.session_state.proc_msg = None
    if 'selected_rack' not in st.session_state: st.session_state.selected_rack = None

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
    
    # 1. 매핑 확인
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

    # 2. 압축코드 확인
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

@st.fragment
def view_inventory_dashboard(df_log, df_mapping, df_master, df_details):
    if df_log.empty:
        st.info("데이터 없음")
        return

    stock_boxes, merged, filtered_details = calculate_stock_snapshot(df_log, df_mapping, df_master, df_details)

    req_cols = ['날짜', '구분', '입고구분', 'box번호', '위치', '파렛트', '품목코드', '규격', '공급업체', '수량']
    final_cols = [c for c in req_cols if c in merged.columns]
    
    d1, d2, d3 = st.columns(3)
    with d1: st.download_button("📥 재고 요약 다운로드", to_excel(merged[final_cols]), "재고요약.xlsx", use_container_width=True)
    with d2: st.download_button("📥 상세 내역 다운로드 (재고분)", to_excel(filtered_details), "상세내역_재고.xlsx", use_container_width=True)
    
    st.divider()
    sc1, sc2, sc3 = st.columns([1, 1, 2])
    with sc1: search_target = st.selectbox("검색 기준", ["전체", "품목코드", "규격", "box번호"])
    with sc2: exact_match = st.checkbox("정확히 일치", value=True)
    with sc3: search_query = st.text_input("검색어", key="sq")

    filtered_df = merged
    hl_list = []

    if search_query and not filtered_df.empty:
        q = search_query.strip().upper()
        if search_target == "전체":
            if exact_match:
                mask = ((filtered_df['품목코드'] == q) | (filtered_df['품명'] == q) | (filtered_df['box번호'] == q) | (filtered_df['규격'] == q))
            else:
                mask = (filtered_df['품목코드'].astype(str).str.contains(q, na=False) | filtered_df['품명'].astype(str).str.contains(q, na=False) | filtered_df['box번호'].astype(str).str.contains(q, na=False) | filtered_df['규격'].astype(str).str.contains(q, na=False))
        else:
            if exact_match: mask = filtered_df[search_target] == q
            else: mask = filtered_df[search_target].astype(str).str.contains(q, na=False)
        
        filtered_df = filtered_df[mask]
        for loc in filtered_df['위치'].unique():
            clean_loc = str(loc).strip()
            if '-' in clean_loc and '통로' not in clean_loc:
                parts = clean_loc.split('-')
                if len(parts) >= 3: hl_list.append(f"{parts[0]}-{parts[2]}")
                elif len(parts) == 2: hl_list.append(f"{parts[0]}-{parts[1]}")
            else: hl_list.append(clean_loc)
    
    if st.session_state.selected_rack and not filtered_df.empty:
        sel = st.session_state.selected_rack
        hl_list.append(sel)
        def filter_loc(l):
            l = str(l).strip()
            if '통로' in sel: return l == sel
            else:
                if '-' in l and '통로' not in l: return l.startswith(sel.split('-')[0]) and l.endswith(sel.split('-')[-1])
                return False
        filtered_df = filtered_df[filtered_df['위치'].apply(filter_loc)]

    c_map, c_list = st.columns([1.5, 1])
    with c_map:
        st.markdown("##### 🗺️ 창고 배치도")
        rack_summary = {}
        if not stock_boxes.empty and '위치' in stock_boxes.columns:
            locs = stock_boxes['위치'].astype(str).str.strip()
            for raw_loc in locs:
                if not raw_loc or raw_loc == '미지정': continue
                if '통로' in raw_loc: rack_summary[raw_loc] = rack_summary.get(raw_loc, 0) + 1
                else:
                    parts = raw_loc.split('-')
                    if len(parts) >= 3: k = f"{parts[0]}-{parts[2]}"
                    elif len(parts) == 2: k = f"{parts[0]}-{parts[1]}"
                    else: k = raw_loc
                    rack_summary[k] = rack_summary.get(k, 0) + 1

        st.markdown("""
        <style>
        div[data-testid="column"] button { width: 100%; height: 40px !important; margin: 1px 0px !important; padding: 0px !important; font-size: 10px !important; font-weight: 700 !important; border-radius: 4px !important; border: 1px solid #ccc; }
        div[data-testid="column"] button:hover { border-color: #333 !important; transform: scale(1.05); z-index: 5; }
        button[kind="primary"] { background-color: #ffcdd2 !important; color: #b71c1c !important; border: 2px solid #d32f2f !important; }
        button[kind="secondary"] { background-color: #ffffff !important; color: #555 !important; }
        .rack-spacer { height: 10px; width: 100%; } 
        .rack7-label { text-align: center; font-weight: bold; color: #555; margin-bottom: 5px; font-size: 12px; }
        </style>
        """, unsafe_allow_html=True)

        def rack_click(key):
            st.session_state.selected_rack = key

        def aisle_btn(name):
            qty = rack_summary.get(name, 0)
            label = f"{name}\n({qty})" if qty > 0 else name
            is_hl = (name in hl_list)
            st.button(label, key=f"btn_{name}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(name,), use_container_width=True)

        cl, cm, cr = st.columns([3.5, 0.1, 1.2]) 
        with cl:
            def rack_row(r_num):
                cols = st.columns(7)
                for c_idx, col in enumerate(cols):
                    rack_key = f"{r_num}-{c_idx+1}"
                    qty = rack_summary.get(rack_key, 0)
                    label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                    is_hl = (rack_key in hl_list)
                    col.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,))
            rack_row(6); aisle_btn("5~6 통로")
            rack_row(5); st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
            rack_row(4); aisle_btn("3~4 통로")
            rack_row(3); st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
            rack_row(2); aisle_btn("1~2 통로")
            rack_row(1)
        with cr:
            st.markdown('<div class="rack7-label">Rack 7 & Aisle</div>', unsafe_allow_html=True)
            c_r7, c_a7 = st.columns([1, 1])
            with c_r7:
                for i in range(12, 0, -1):
                    rack_key = f"7-{i}"
                    qty = rack_summary.get(rack_key, 0)
                    label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                    is_hl = (rack_key in hl_list)
                    st.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,), use_container_width=True)
            with c_a7: aisle_btn("7번 통로")

    with c_list:
        st.markdown(f"##### 📋 재고 리스트 ({len(filtered_df)}건)")
        final_cols_disp = [c for c in req_cols if c in filtered_df.columns]
        st.dataframe(filtered_df[final_cols_disp], use_container_width=True, height=600)

@st.dialog("일정 관리")
def schedule_dialog(sel_date=None, event_data=None):
    if event_data:
        st.subheader("일정 수정/삭제")
        new_title = st.text_input("업체명 / 내용", value=event_data["title"])
        try:
            dt_obj = datetime.fromisoformat(event_data["start"])
            d_val, t_val = dt_obj.date(), dt_obj.time()
        except:
            d_val, t_val = datetime.today().date(), datetime.now().time()
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
    else:
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

# --- 메인 ---
def main():
    init_session_state()
    df_master, df_mapping, df_log, df_details = load_data_from_db()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["1. 연속 스캔", "2. 재고 현황", "3. 일괄 업로드", "4. 포장데이터", "5. 품목 마스터", "6. 데이터 진단", "7. 월간 일정"])

    with tab1:
        c_h, c_r = st.columns([4, 1])
        with c_h: st.subheader("🚀 스캔 작업")
        with c_r: 
            if st.button("🔄 새로고침", use_container_width=True, key='r1'): clear_cache(); st.rerun()

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
                if not col_box:
                    st.error("❌ 'Box번호' 컬럼이 없습니다.")
                    st.stop()
                
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
                    st.error(f"⛔ 업로드 불가: 다음 박스들은 현재 재고에 없어 출고할 수 없습니다.\n{missing_boxes[:10]} ...")
                    st.stop()

                if chunked_insert('입출고', clean_df):
                    st.success(f"✅ 총 {len(clean_df)}건 업로드 완료!")
                    clear_cache()
                    st.rerun()
            except Exception as e:
                st.error(f"업로드 중 오류 발생: {e}")

    with tab4:
        st.subheader("📦 포장데이터(마스터) 등록 (대용량)")
        with st.expander("🚨 데이터 전체 초기화 (주의)"):
            st.warning("이 버튼을 누르면 모든 데이터가 삭제됩니다.")
            if st.button("데이터 초기화 실행", type="primary"):
                if reset_database():
                    st.success("모든 데이터가 삭제되었습니다.")
                    st.rerun()

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

                st.write("품목표 업로드 중...")
                chunked_upsert('품목표', items, '품목코드')
                
                st.write("매핑정보 업로드 중...")
                chunked_upsert('매핑정보', grp, 'box번호')
                
                if not dets.empty:
                    st.write("상세내역 업로드 중...")
                    chunked_insert('상세내역', dets)
                
                clear_cache()
                st.success("✅ 대용량 등록 완료!")
                st.rerun()
            except Exception as e: st.error(f"오류: {e}")

    with tab5:
        st.dataframe(df_master.head(1000))

    with tab6:
        st.subheader("🕵️‍♀️ 데이터 진단 (총량 확인)")
        c1, c2, c3 = st.columns(3)
        c1.metric("품목표", f"{len(df_master)}건")
        c2.metric("매핑정보", f"{len(df_mapping)}건")
        c3.metric("입출고", f"{len(df_log)}건")
        st.write("▼ 매핑정보 샘플")
        st.dataframe(df_mapping.head(50))

    with tab7:
        st.subheader("🗓️ 월간 출고 일정")
        try:
            from streamlit_calendar import calendar
            events = fetch_schedules()
            
            # 캘린더가 렌더링될 데이터가 없어도 달력은 보여야 함
            # 기본 이벤트 리스트가 비어있어도 함수는 실행됨
            cal = calendar(
                events=events,
                options={
                    "headerToolbar": {"left": "today prev,next", "center": "title", "right": "dayGridMonth,timeGridWeek,timeGridDay"},
                    "initialView": "dayGridMonth",
                },
                key="my_calendar"
            )
            
            # 캘린더 라이브러리가 로드되었음을 표시 (진단용)
            # st.caption("캘린더 모듈 정상 작동 중") 

            if cal.get("callback") == "dateClick":
                schedule_dialog(sel_date=cal["dateClick"]["date"])
            elif cal.get("callback") == "eventClick":
                evt_id = cal["eventClick"]["event"]["id"]
                evt_data = next((e for e in events if e["id"] == evt_id), None)
                if evt_data: schedule_dialog(event_data=evt_data)
        except ImportError:
            st.error("❌ 'streamlit-calendar' 라이브러리가 설치되지 않았습니다. requirements.txt를 확인해주세요.")
        except Exception as e:
            st.error(f"❌ 캘린더 로드 중 오류 발생: {e}")

if __name__ == '__main__':
    main()
