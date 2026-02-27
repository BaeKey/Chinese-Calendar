import json
import requests
import os
import hashlib
import re
from datetime import datetime, timedelta, timezone
from lunar_python import Solar

# ================= 配置项 =================
DATA_URL = "https://cdn.jsdelivr.net/npm/chinese-days/dist/chinese-days.json"
DATA_FILENAME = "chinese-days.json"
TRADITIONAL_CACHE_FILENAME = "traditional_cache.json" # [新增] 民俗计算缓存文件
OUTPUT_FILENAME = "chinese_holidays.ics"
TZ_ID = "Asia/Shanghai"

TRADITIONAL_START_YEAR = 2025
TRADITIONAL_END_YEAR = 2050

# 1. 公历固定节日
FIXED_FESTIVALS_SOLAR = {
    "01-10": "中国人民警察节",
    "02-14": "情人节",
    "03-08": "妇女节",
    "03-12": "植树节",
    "03-15": "消费者权益日",
    "04-01": "愚人节",
    "04-22": "世界地球日",
    "04-23": "世界读书日",
    "05-04": "青年节",
    "05-12": "护士节",
    "06-01": "儿童节",
    "06-05": "世界环境日",
    "06-26": "国际禁毒日",
    "07-01": "建党节",
    "07-07": "七七事变",
    "08-01": "建军节",
    "08-15": "日本投降日",
    "09-03": "抗战胜利纪念日",
    "09-10": "教师节",
    "09-18": "九一八事变",
    "09-30": "烈士纪念日",
    "10-01": "国庆节",
    "10-10": "辛亥革命纪念日",
    "10-24": "程序员节",
    "10-25": "台湾光复纪念日",
    "10-31": "万圣夜",
    "11-08": "记者节",
    "12-13": "国家公祭日",
    "12-20": "澳门回归纪念日",
    "12-24": "平安夜",
    "12-25": "圣诞节"
}

# 2. 农历固定节日
FIXED_FESTIVALS_LUNAR = {
    (1, 15): "元宵节",
    (2, 2):  "龙抬头",
    (3, 3):  "上巳节",
    (5, 5):  "端午节",
    (7, 7):  "七夕节",
    (7, 15): "中元节",
    (8, 15): "中秋节",
    (9, 9):  "重阳节",
    (10, 1): "寒衣节",
    (10, 15): "下元节",
    (12, 8): "腊八节",
    (12, 16): "尾牙",
    (12, 23): "北方小年",
    (12, 24): "南方小年",
}

# ================= 辅助函数 =================

def get_week_name(date_obj):
    weeks = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    return weeks[date_obj.weekday()]

def format_ics_date(dt_obj, is_full_day=True):
    if is_full_day:
        return dt_obj.strftime("%Y%m%d")
    return dt_obj.strftime("%Y%m%dT%H%M%S")

def generate_uid(date_str, suffix):
    return f"{date_str}_{suffix}@365day.top"

def get_now_utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get_now_display():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def calculate_file_md5(filepath):
    if not os.path.exists(filepath):
        return None
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def fold_line(text):
    result = []
    while len(text) > 50:
        result.append(text[:50])
        text = " " + text[50:]
    result.append(text)
    return "\r\n".join(result)

class CalendarGenerator:
    def __init__(self):
        self.events = []
        self.raw_data = {}
        self.holiday_groups = {} 
        self.traditional_cache_list = [] # [新增] 用于暂存计算结果
        
    def ensure_data_file(self):
        print(f"正在检查法定假日数据更新: {DATA_URL}")
        try:
            resp = requests.get(DATA_URL, timeout=30)
            resp.raise_for_status()
            remote_content = resp.content
            remote_hash = hashlib.md5(remote_content).hexdigest()
            local_hash = calculate_file_md5(DATA_FILENAME)
            
            if local_hash != remote_hash:
                print("发现法定假日新版本或本地缺失，正在写入...")
                with open(DATA_FILENAME, 'wb') as f:
                    f.write(remote_content)
            else:
                print("法定假日本地缓存已是最新。")
            
            self.raw_data = json.loads(remote_content)
            
        except Exception as e:
            print(f"网络请求或写入失败: {e}")
            if os.path.exists(DATA_FILENAME):
                with open(DATA_FILENAME, 'r', encoding='utf-8') as f:
                    self.raw_data = json.load(f)
            else:
                print("程序退出。")
                exit(1)

    def parse_holidays(self):
        holidays_map = self.raw_data.get('holidays', {})
        workdays_map = self.raw_data.get('workdays', {})
        temp_groups = {}

        def process_dates(date_map, is_workday):
            for date_str, val in date_map.items():
                cn_name = ""
                if isinstance(val, str):
                    parts = val.split(',')
                    if len(parts) >= 2: cn_name = parts[1]
                elif isinstance(val, dict):
                    cn_name = val.get('name', '')
                
                if not cn_name: continue
                if cn_name not in temp_groups:
                    temp_groups[cn_name] = {'holidays': [], 'workdays': []}
                
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                    if is_workday: temp_groups[cn_name]['workdays'].append(dt)
                    else: temp_groups[cn_name]['holidays'].append(dt)
                except ValueError: continue

        process_dates(holidays_map, is_workday=False)
        process_dates(workdays_map, is_workday=True)
        if not holidays_map and not workdays_map and self.raw_data:
            process_dates(self.raw_data, is_workday=False)

        self.holiday_groups = temp_groups

    def get_consecutive_blocks(self, dates):
        if not dates: return []
        sorted_dates = sorted(list(set(dates)))
        blocks = []
        current_block = [sorted_dates[0]]
        for i in range(1, len(sorted_dates)):
            diff = sorted_dates[i] - sorted_dates[i-1]
            if diff.days == 1: current_block.append(sorted_dates[i])
            else:
                blocks.append(current_block)
                current_block = [sorted_dates[i]]
        blocks.append(current_block)
        return blocks

    def process_holiday_events(self):
        now_stamp = get_now_utc_stamp()
        for name, data in self.holiday_groups.items():
            all_h_dates = data['holidays']
            all_w_dates = sorted(list(set(data['workdays'])))
            blocks = self.get_consecutive_blocks(all_h_dates)
            
            for block in blocks:
                start_dt = block[0]
                end_dt = block[-1]
                
                related_workdays = []
                check_start = start_dt - timedelta(days=20)
                check_end = end_dt + timedelta(days=20)
                for wd in all_w_dates:
                    if check_start <= wd <= check_end: related_workdays.append(wd)
                
                description = self.generate_block_description(name, block, related_workdays)
                ics_end_dt = end_dt + timedelta(days=1)
                
                self.events.append({
                    "dtstart": format_ics_date(start_dt),
                    "dtend": format_ics_date(ics_end_dt),
                    "uid": generate_uid(start_dt.strftime("%Y%m%d"), "holiday_block"),
                    "created": now_stamp,
                    "description": description,
                    "summary": f"{name} 假期",
                    "status": "CONFIRMED",
                    "transp": "TRANSPARENT",
                    "is_allday": True
                })

                for i, wd in enumerate(related_workdays):
                    w_summary = f"{name} 补班"
                    w_start = wd.replace(hour=9, minute=0, second=0)
                    w_end = wd.replace(hour=18, minute=0, second=0)
                    
                    self.events.append({
                        "dtstart": format_ics_date(w_start, False),
                        "dtend": format_ics_date(w_end, False),
                        "uid": generate_uid(wd.strftime("%Y%m%d"), f"work_{i}"),
                        "created": now_stamp,
                        "description": description,
                        "summary": w_summary,
                        "status": "TENTATIVE",
                        "transp": "OPAQUE",
                        "is_allday": False,
                        "alarm": f"补班提醒：{w_summary}"
                    })

    def generate_block_description(self, name, h_dates, w_dates):
        if not h_dates: return ""
        start, end, count = h_dates[0], h_dates[-1], len(h_dates)
        desc = f"{name}：{start.month}月{start.day}日（{get_week_name(start)}）"
        if count > 1: desc += f"至{end.day}日（{get_week_name(end)}）放假调休"
        else: desc += "放假"
        desc += f"，共{count}天。"
        if w_dates:
            w_desc_list = [f"{wd.month}月{wd.day}日（{get_week_name(wd)}）" for wd in w_dates]
            desc += " " + "、".join(w_desc_list) + "上班。"
        return desc

    # [新增] 专门用于记录缓存的包裹函数
    def _record_traditional_event(self, start_dt, end_dt, summary, description="", is_allday=True):
        self.traditional_cache_list.append({
            "start": start_dt.strftime("%Y%m%d"),
            "end": end_dt.strftime("%Y%m%d"),
            "summary": summary,
            "description": description,
            "is_allday": is_allday
        })
        self.create_event(start_dt, end_dt, summary, description, is_allday)

    def add_traditional_events(self):
        # [新增] 1. 优先检查并读取本地缓存
        if os.path.exists(TRADITIONAL_CACHE_FILENAME):
            try:
                with open(TRADITIONAL_CACHE_FILENAME, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                # 校验缓存的年份范围是否匹配，如果匹配则直接加载
                if cache_data.get("start_year") == TRADITIONAL_START_YEAR and \
                   cache_data.get("end_year") == TRADITIONAL_END_YEAR:
                    print("检测到匹配的民俗/传统节日缓存，直接加载，跳过复杂计算...")
                    for ev in cache_data.get("events", []):
                        start_dt = datetime.strptime(ev["start"], "%Y%m%d")
                        end_dt = datetime.strptime(ev["end"], "%Y%m%d")
                        self.create_event(start_dt, end_dt, ev["summary"], ev["description"], ev.get("is_allday", True))
                    return
                else:
                    print("缓存年份范围已改变，准备重新计算...")
            except Exception as e:
                print(f"读取民俗缓存失败: {e}，将重新计算...")

        # 2. 如果无缓存或缓存失效，则进行逐日计算
        print(f"正在深度计算民俗/动态/传统节日 ({TRADITIONAL_START_YEAR}-{TRADITIONAL_END_YEAR})，这需要一点时间...")
        self.traditional_cache_list = [] # 清空缓存列表准备记录

        for year in range(TRADITIONAL_START_YEAR, TRADITIONAL_END_YEAR + 1):
            for date_str, name in FIXED_FESTIVALS_SOLAR.items():
                try:
                    dt = datetime.strptime(f"{year}-{date_str}", "%Y-%m-%d")
                    
                    # 1. 处理香港回归 (7月1日与建党节同日，需要保留建党节并额外添加回归日)
                    if date_str == "07-01":
                        self._record_traditional_event(dt, dt + timedelta(days=1), name, "公历节日") # 保留建党节
                        anniversary = year - 1997
                        if anniversary > 0: 
                            self._record_traditional_event(dt, dt + timedelta(days=1), f"香港回归纪念日({anniversary}周年)", "纪念日")
                            
                    # 2. 处理澳门回归
                    elif date_str == "12-20":
                        anniversary = year - 1999
                        if anniversary > 0: 
                            self._record_traditional_event(dt, dt + timedelta(days=1), f"澳门回归纪念日({anniversary}周年)", "纪念日")
                        else:
                            self._record_traditional_event(dt, dt + timedelta(days=1), name, "公历节日") # 1999年及以前兜底用原名
                            
                    # 3. 其他常规公历节日正常添加
                    else:
                        self._record_traditional_event(dt, dt + timedelta(days=1), name, "公历节日")
                        
                except ValueError: pass

            self.create_dynamic_solar_event(year, 5, 6, 2, "母亲节")
            self.create_dynamic_solar_event(year, 6, 6, 3, "父亲节")
            tg_date = self.create_dynamic_solar_event(year, 11, 3, 4, "感恩节")
            if tg_date:
                bf_date = tg_date + timedelta(days=1)
                self._record_traditional_event(bf_date, bf_date + timedelta(days=1), "黑色星期五", "商业节日")

            curr = datetime(year, 1, 1)
            end_dt = datetime(year, 12, 31)
            looking_for_rumei = False
            looking_for_chumei = False
            
            while curr <= end_dt:
                solar = Solar.fromYmd(curr.year, curr.month, curr.day)
                lunar = solar.getLunar()
                l_month, l_day = lunar.getMonth(), lunar.getDay()
                
                jie_qi = lunar.getJieQi()
                if jie_qi:
                    self._record_traditional_event(curr, curr + timedelta(days=1), jie_qi, "二十四节气")
                    if jie_qi == "芒种": looking_for_rumei = True
                    if jie_qi == "小暑": looking_for_chumei = True

                if looking_for_rumei and lunar.getDayGan() == "丙":
                    self._record_traditional_event(curr, curr + timedelta(days=1), "入梅", "节气民俗")
                    looking_for_rumei = False 
                
                if looking_for_chumei and lunar.getDayZhi() == "未":
                    self._record_traditional_event(curr, curr + timedelta(days=1), "出梅", "节气民俗")
                    looking_for_chumei = False 

                if lunar.getShuJiu() and lunar.getShuJiu().getIndex() == 1:
                    name = lunar.getShuJiu().getName()
                    if name in ["一九", "二九", "三九", "四九", "五九", "六九", "七九", "八九", "九九"]:
                        title = "一九" if name == "一九" else name
                        self._record_traditional_event(curr, curr + timedelta(days=1), title, "节气民俗")

                if lunar.getFu() and lunar.getFu().getIndex() == 1:
                    fu_name = "入伏" if lunar.getFu().getName() == "初伏" else lunar.getFu().getName()
                    self._record_traditional_event(curr, curr + timedelta(days=1), fu_name, "节气民俗")

                if l_day == 1:
                    m_name = lunar.getMonthInChinese()
                    if m_name in ["正", "冬", "腊"]:
                        self._record_traditional_event(curr, curr + timedelta(days=1), f"进入{m_name}月", "农历月份")

                if (l_month, l_day) in FIXED_FESTIVALS_LUNAR:
                    self._record_traditional_event(curr, curr + timedelta(days=1), FIXED_FESTIVALS_LUNAR[(l_month, l_day)], "传统节日")
                
                tomorrow_lunar = Solar.fromYmd((curr + timedelta(days=1)).year, (curr + timedelta(days=1)).month, (curr + timedelta(days=1)).day).getLunar()
                if tomorrow_lunar.getJieQi() == "清明":
                    self._record_traditional_event(curr, curr + timedelta(days=1), "寒食节", "传统节日")

                if l_month == 1 and l_day == 1:
                    self._record_traditional_event(curr, curr + timedelta(days=1), "春节", "传统节日")
                if tomorrow_lunar.getMonth() == 1 and tomorrow_lunar.getDay() == 1:
                    self._record_traditional_event(curr, curr + timedelta(days=1), "除夕", "传统节日")

                curr += timedelta(days=1)
                
        # [新增] 3. 循环结束后，将记录的内容写入本地缓存文件
        print("计算完毕，正在保存民俗/传统节日缓存...")
        cache_data = {
            "start_year": TRADITIONAL_START_YEAR,
            "end_year": TRADITIONAL_END_YEAR,
            "events": self.traditional_cache_list
        }
        with open(TRADITIONAL_CACHE_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

    def create_dynamic_solar_event(self, year, month, target_weekday, nth, name):
        first_day = datetime(year, month, 1)
        delta_days = (target_weekday - first_day.weekday() + 7) % 7
        target_date = first_day + timedelta(days=delta_days) + timedelta(weeks=nth-1)
        
        if target_date.month == month:
            self._record_traditional_event(target_date, target_date + timedelta(days=1), name, "公历动态节日")
            return target_date
        return None

    def create_event(self, start_dt, end_dt, summary, description="", is_allday=True):
        unique_str = f"{start_dt.strftime('%Y%m%d')}-{summary}"
        uid_hash = hashlib.md5(unique_str.encode()).hexdigest()[:12]
        
        self.events.append({
            "dtstart": format_ics_date(start_dt, is_allday),
            "dtend": format_ics_date(end_dt, is_allday),
            "uid": generate_uid(start_dt.strftime("%Y%m%d"), uid_hash),
            "created": get_now_utc_stamp(),
            "description": description,
            "summary": summary,
            "status": "CONFIRMED",
            "transp": "TRANSPARENT",
            "is_allday": is_allday
        })

    def generate_ics_content(self, update_time_str):
        lines = [
            "BEGIN:VCALENDAR",
            "PRODID:-//365day.top//China Public Holidays 2.0//CN",
            "VERSION:2.0",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:中国节假日",
            f"X-WR-TIMEZONE:{TZ_ID}",
            f"X-WR-CALDESC:{TRADITIONAL_START_YEAR}~{TRADITIONAL_END_YEAR}年中国放假、调休和补班日历 更新时间{update_time_str}",
            "REFRESH-INTERVAL;VALUE=DURATION:PT24H",
            "X-PUBLISHED-TTL:PT24H",
            "X-APPLE-CALENDAR-COLOR:#E62325",
            "BEGIN:VTIMEZONE",
            f"TZID:{TZ_ID}",
            f"X-LIC-LOCATION:{TZ_ID}",
            "BEGIN:STANDARD",
            "TZOFFSETFROM:+0800",
            "TZOFFSETTO:+0800",
            "TZNAME:CST",
            "DTSTART:19700101T000000",
            "END:STANDARD",
            "END:VTIMEZONE"
        ]
        
        self.events.sort(key=lambda x: x['dtstart'])
        now_stamp = get_now_utc_stamp()
        
        for ev in self.events:
            lines.append("BEGIN:VEVENT")
            if ev['is_allday']:
                lines.append(f"DTSTART;VALUE=DATE:{ev['dtstart']}")
                lines.append(f"DTEND;VALUE=DATE:{ev['dtend']}")
            else:
                lines.append(f"DTSTART;TZID={TZ_ID}:{ev['dtstart']}")
                lines.append(f"DTEND;TZID={TZ_ID}:{ev['dtend']}")
                
            lines.append(f"DTSTAMP:{now_stamp}")
            lines.append(f"UID:{ev['uid']}")
            lines.append(f"CREATED:{ev['created']}")
            if ev['description']: lines.append(fold_line(f"DESCRIPTION:{ev['description']}"))
            lines.append(f"LAST-MODIFIED:{now_stamp}")
            lines.append(f"STATUS:{ev['status']}")
            lines.append(fold_line(f"SUMMARY:{ev['summary']}"))
            lines.append(f"TRANSP:{ev['transp']}")
            
            if 'alarm' in ev:
                lines.append("BEGIN:VALARM")
                lines.append("TRIGGER:-P1D")
                lines.append("ACTION:DISPLAY")
                lines.append(fold_line(f"DESCRIPTION:{ev['alarm']}"))
                lines.append("END:VALARM")
            
            lines.append("END:VEVENT")
            
        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    def save_file(self):
        current_display_time = get_now_display()
        new_content_full = self.generate_ics_content(current_display_time)
        old_content = ""
        old_display_time = ""
        
        if os.path.exists(OUTPUT_FILENAME):
            with open(OUTPUT_FILENAME, 'r', encoding='utf-8') as f:
                old_content = f.read()
            match = re.search(r"更新时间(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", old_content)
            if match: old_display_time = match.group(1)
        
        if self.is_content_same(old_content, new_content_full):
            print("文件内容无实质变化，保持更新时间不变。")
            if old_display_time: final_content = self.generate_ics_content(old_display_time)
            else: final_content = new_content_full
        else:
            print(f"检测到内容更新，更新时间戳为：{current_display_time}")
            final_content = new_content_full
            
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8', newline='') as f:
            f.write(final_content)

    def is_content_same(self, old_text, new_text):
        if not old_text: return False
        def clean(text):
            text = re.sub(r"更新时间\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "", text)
            text = re.sub(r"DTSTAMP:.*", "", text)
            text = re.sub(r"LAST-MODIFIED:.*", "", text)
            text = re.sub(r"CREATED:.*", "", text)
            return text.strip()
        return clean(old_text) == clean(new_text)

    def run(self):
        self.ensure_data_file()
        self.parse_holidays()
        self.process_holiday_events()
        self.add_traditional_events()
        self.save_file()
        print(f"全部生成完成: {OUTPUT_FILENAME}")

if __name__ == "__main__":
    generator = CalendarGenerator()
    generator.run()