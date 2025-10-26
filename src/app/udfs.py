# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re, unicodedata
import patterns
import math

@udf(returnType=ArrayType(StringType()))
# trả về những framework mà công việc đó yêu cầu
def extract_framework_plattform(mo_ta_cong_viec, yeu_cau_ung_vien):
    frameworks=[]
    for framework in patterns.framework_plattforms:
        if re.search(framework, mo_ta_cong_viec+" "+ yeu_cau_ung_vien,re.IGNORECASE):
            frameworks.append(framework)
    return frameworks

@udf(returnType=ArrayType(StringType()))
def extract_IT_language(mo_ta_cong_viec, yeu_cau_ung_vien):
    languages= []
    for language in patterns.IT_languages:
        formal_language= language.replace("+", "\+").replace("(", "\(").replace(")", "\)")
        if re.search(formal_language, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE):
            languages.append(language)
    return languages

@udf(returnType=ArrayType(StringType()))
def extract_language(yeu_cau_ung_vien):
    languages=[]
    for language, normalize_language in patterns.languages.items():
        if re.search(language, yeu_cau_ung_vien, re.IGNORECASE):
            languages.append(normalize_language)
    return languages



@udf(returnType=ArrayType(StringType()))
def extract_knowledge(mo_ta_cong_viec, yeu_cau_ung_vien):
    knowledges = []
    for knowledge in patterns.knowledges:
        if re.search(knowledge, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE):
            knowledges.append(knowledge)
    return knowledges


@udf(returnType=ArrayType(StringType()))
def extract_design_pattern(mo_ta_cong_viec, yeu_cau_ung_vien):
    design_patterns = []
    for design_pattern in patterns.design_patterns:
        if re.search(design_pattern, mo_ta_cong_viec + " " + yeu_cau_ung_vien, re.IGNORECASE):
            design_patterns.append(design_pattern)
    return design_patterns


@udf(returnType=ArrayType(IntegerType()))
def extract_exp_pattern(kinh_nghiem):
    # lấy ra tất cả các số nguyên <-> số năm kinh nghiệm
    exp = re.findall(r'\b\d+\b', kinh_nghiem)
    exp_range = [int(number) for number in exp]
    if len(exp_range) == 2:
        end= exp_range[1]
        start = exp_range[0]
        if end >= 10:
            end = 10

        start = int(start / 1)
        end = int(end / 1)

        exp_range= [1 * i for i in range(start, end + 1)]

    return exp_range


@udf(returnType=ArrayType(IntegerType()))
def extract_old_pattern(tuoi):
    if tuoi is not None:
        old = re.findall(r'\b\d+\b', tuoi)
        old_range = [int(number) for number in old]
        if len(old_range) == 2:
            end = old_range[1]
            start = old_range[0]
            if end >= 45:
                end = 45

            start = int(start / 5)
            end = int(end / 5)

            old_range = [5 * i for i in range(start, end + 1)]
        return old_range
    else:
        return []

@udf(returnType=ArrayType(IntegerType()))
def normalize_salary_vals(sal_min, sal_max, currency):
    if sal_min is None and sal_max is None:
        return []
    BIN = 5  # bước 5 triệu VND
    CAP = 100  # trần 100 triệu

    def sal_to_bins(v_million):
        if v_million is None:
            return []
        b = int(v_million // BIN)
        return [min(BIN * b, CAP)]

    def range_to_bins(a_million, b_million):
        if a_million is None and b_million is None:
            return []
        if a_million is None:  # chỉ có max
            return sal_to_bins(b_million)
        if b_million is None:  # chỉ có min
            return sal_to_bins(a_million)
        if a_million > b_million:
            a_million, b_million = b_million, a_million
        a = int(a_million // BIN)
        b = int(min(b_million, CAP) // BIN)
        return [BIN * i for i in range(a, b + 1)]

    # đổi đơn vị về "triệu VND"
    try:
        if currency and str(currency).upper() == "USD":
            # 1 USD ≈ 24k VND → triệu VND = usd*24/1000
            to_million = lambda x: math.floor(float(x) * 24 / 1000) if x is not None else None
        else:
            to_million = lambda x: math.floor(float(x) / 1_000_000) if x is not None else None

        a_million = to_million(sal_min)
        b_million = to_million(sal_max)

        # nếu min==max → trả 1 bin; nếu khác → trả dải bin
        if a_million is not None and b_million is not None and a_million == b_million:
            return sal_to_bins(a_million)
        return range_to_bins(a_million, b_million)
    except Exception:
        return []

@udf(returnType=ArrayType(IntegerType()))
def normalize_salary(quyen_loi):
    BIN_SIZE = 5

    def extract_salary(quyen_loi):
        '''
        Return a list of salary patterns found in raw data

        Parameters
        ----------
        quyen_loi : quyen_loi field in raw data
        '''
        salaries = []
        # salaries= quyen_loi.split()
        for pattern in patterns.salary_patterns:
            salaries.extend(re.findall(pattern, unicodedata.normalize('NFKC', quyen_loi), re.IGNORECASE))
        return salaries

    def sal_to_bin_list(sal):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        sal : salary value
        '''
        sal = int(sal / BIN_SIZE)
        if sal < int(100 / BIN_SIZE):
            return [BIN_SIZE * sal]
        else:
            return [100]

    def range_to_bin_list(start, end):
        '''
        Return a list of bin containing salary range

        Parameters
        ----------
        start : the start of salary range
        end : the end of salary range
        '''
        if end >= 100:
            end = 100

        start = int(start / BIN_SIZE)
        end = int(end / BIN_SIZE)

        return [BIN_SIZE * i for i in range(start, end + 1)]

    def dollar_to_vnd(dollar):
        '''
        Return a list of bin containing salary value

        Parameters
        ----------
        dollar : salary value in dollar unit
        '''
        return sal_to_bin_list(math.floor(dollar * 24 / 1000))

    def dollar_handle(currency):
        '''
        Handle currency
        If currency is in dollar unit, returns the salary bins
        Otherwise returns None

        Parameter
        ---------
        currency : string of salary pattern
        '''
        if not currency.__contains__("$"):
            if not currency.__contains__("USD"):
                if not currency.__contains__("usd"):
                    return None
                else:
                    ext_curr = currency.replace("usd", "")
            else:
                ext_curr = currency.replace("USD", "")
        elif (currency.startswith("$")):
            ext_curr = currency[1:]
        else:
            ext_curr = currency[:-1]
        ext_curr = ext_curr.replace(",", "")
        try:
            val_curr = int(ext_curr)
            return dollar_to_vnd(val_curr)
        except ValueError:
            return None

    def normalize_vnd(vnd):
        '''
        Return normalized currency in VND unit
        Normalize currency is a string of currency in milion VND unit
        The postfix such as Triệu, triệu, M, m,... is removed

        Parameters
        ----------
        vnd : string of salary in vnd unit
        '''
        mill = "000000"
        norm_vnd = vnd.replace("triệu", mill).replace("Triệu", mill) \
            .replace("TRIỆU", mill).replace("m", mill).replace("M", mill) \
            .replace(".", "").replace(" ", "").replace(",", "")
        try:
            vnd = math.floor(int(norm_vnd) / 1000000)
            return vnd
        except ValueError:
            print("Value Error while converting ", norm_vnd)
            return None

    def vnd_handle(ori_range_list):
        '''
        Handle currency, returns the salary bins
        The currency must be preprocessed and returned None by dollar_handle()
        The currency must be stripped and splitted by "-" to become a list

        Parameters
        ----------
        ori_range_list : the range of salary (a list containing at most 2 element)
        '''
        if (len(ori_range_list) == 1):
            sal = normalize_vnd(ori_range_list[0])
            if sal != None:
                return sal_to_bin_list(sal)
        else:
            try:
                start = int(ori_range_list[0].strip().replace(".", "").replace(",", ""))
                end = normalize_vnd(ori_range_list[1])
                if end != None:
                    return range_to_bin_list(start, end)

                else:
                    print("Error converting end ", ori_range_list[1], " with start ", ori_range_list[0])
            except ValueError:
                print("Error Converting Start ", ori_range_list[0], " with end ", ori_range_list[1])
            return None
        # return [0]*11
        return None

    def salary_handle(currency):
        '''
        Handle currency
        Return salary bin

        Parameters
        ----------
        currency : a string
        '''
        range_val = dollar_handle(currency)
        if (range_val == None):
            splitted_currency = currency.strip().strip("-").split("-")
            range_val = vnd_handle(splitted_currency)
        return range_val

    salaries = extract_salary(quyen_loi)
    bin_set = set()
    for sal in salaries:
        sal_bins = salary_handle(sal)
        if sal_bins != None and sal_bins != []:
            bin_set = bin_set.union(tuple(sal_bins))
    bin_set = sorted(list(bin_set))

    if len(bin_set) == 2:
        bin_set = range_to_bin_list(int(bin_set[0]), int(bin_set[1]))

    return bin_set


@udf(returnType=StringType())
def extract_location(dia_diem_cong_viec):
    for province in patterns.provinces:
        if re.search(province, dia_diem_cong_viec, re.IGNORECASE):
            return province
    return None
    # return len(dia_diem_cong_viec)

@udf(returnType=ArrayType(StringType()))
def extract_job_type(nganh_nghe):
    job_type =[]
    if re.search('CNTT - Phần mềm', nganh_nghe, re.IGNORECASE):
        job_type.append('software')
    if re.search('CNTT - Phần cứng / Mạng', nganh_nghe, re.IGNORECASE):
        job_type.append('hardware')
    return job_type


@udf(returnType=StringType())
def get_grouped_knowledge(knowledge):
    for x in knowledge:
        res = patterns.labeled_knowledges.get(x)
        if res is not None:
            return res

@udf(returnType=ArrayType(StringType()))
def extract_education( ki_nang_yeu_cau):
    res=[]
    for edu in patterns.educations:
        if re.search(edu, " "+ki_nang_yeu_cau, re.IGNORECASE):
            res.append(edu)
    return res

def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    return s

def _hours_between(t1: str, t2: str) -> float:
    h1, m1 = map(int, t1.split(":"))
    h2, m2 = map(int, t2.split(":"))
    return max(0, (h2*60 + m2 - (h1*60 + m1)) / 60.0)

def _estimate_hours_per_day(text: str) -> float | None:
    times = re.findall(r'(\d{1,2}:\d{2})', text)
    hours = 0.0
    for i in range(0, len(times)-1, 2):
        hours += _hours_between(times[i], times[i+1])
    if hours == 0:
        m = re.search(r'(\d{1,2})\s*(?:h|gio|giờ|tieng|tiếng)', _norm(text))
        if m:
            hours = float(m.group(1))

    if hours >= 8.5:
        hours -= 1.0
    return hours if hours > 0 else None

def _estimate_days_per_week(text_norm: str) -> float | None:
    # range: "thu 2 - thu 6"
    m = re.search(r'thu\s*([2-7])\s*-\s*thu\s*([2-7])', text_norm)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if b >= a:
            return float(b - a + 1)

    # danh sách rời rạc “thứ 2, thứ 4, thứ 6”
    days = set(int(x) for x in re.findall(r'thu\s*([2-7])', text_norm))
    has_cn = bool(re.search(r'chu\s*nhat|cn', text_norm))
    if days:
        d = len(days) + (1 if has_cn else 0)
        # “2 thu 7/thang” ~ +0.5 ngày/tuần (xấp xỉ)
        if re.search(r'(\d+)\s*thu\s*7\s*/\s*thang', text_norm):
            d = max(d, 5)  # về cơ bản vẫn coi như chế độ full-time
        return float(d)

    return None
@udf(returnType=FloatType())
def classify_work_schedule(text: str) -> str | None:
    if not text:
        return None
    tnorm = _norm(text)

    if "toan thoi gian" in tnorm or "full time" in tnorm:
        return "Toàn thời gian"
    if "ban thoi gian" in tnorm or "part time" in tnorm:
        return "Bán thời gian"

    hpday = _estimate_hours_per_day(text)
    dpw = _estimate_days_per_week(tnorm)

    if hpday is None and dpw is not None:
        hpday = 8.0
    if dpw is None and hpday is not None:
        dpw = 5.0

    if hpday is not None and dpw is not None:
        total = hpday * dpw
        return "Toàn thời gian" if total >= 35 else "Bán thời gian"

    if re.search(r'thu\s*2\s*-\s*thu\s*6', tnorm) or re.search(r'thu\s*2\s*-\s*thu\s*7', tnorm):
        return "Toàn thời gian"

    return "Bán thời gian"