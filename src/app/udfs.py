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
def extract_education(hoc_van, ki_nang_yeu_cau):
    res=[]
    for edu in patterns.educations:
        if re.search(edu, hoc_van+ " "+ki_nang_yeu_cau, re.IGNORECASE):
            res.append(edu)
    return res
