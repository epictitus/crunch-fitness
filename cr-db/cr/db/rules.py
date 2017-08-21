"""
Implement rules for converting and normalizing data column values
"""
import re

from .countries import COUNTRIES


# Column types encapsulate operations to convert str values from the CSV
# file to normalized values. I'm not just using straight converter functions
# because I want to be able to do a reverse lookup of a cateogry string from
# a normalized integer value.

class ColumnType(object):

    def __call__(self, value):
        """Convert column raw string value to normalized value"""
        raise NotImplementedError()


class CategoryColumn(ColumnType):

    def __init__(self, category_map):
        """
        category_map: Map of string category values to integers
        """
        self.category_map_orig = category_map
        self.category_map = dict((k.lower(), v)
                                 for k, v in category_map.iteritems())
        # Inverted map of numbers back to original strings
        self.value_map = {}
        for k, v in self.category_map_orig.iteritems():
            self.value_map[v] = k
        # Sanity checks
        assert len(self.category_map_orig) == len(self.category_map)
        assert len(self.category_map) == len(self.value_map)

    def __call__(self, value):
        return self.category_map.get(value.lower())

    def values(self):
        """Return string category values in mapped numerical order"""
        return [self.value_map[v] for v in sorted(self.value_map)]


class EnumColumn(CategoryColumn):

    def __init__(self, enum_items):
        """
        enum_items:
            sequence of item strings that will be mapped to integers. Order
            matters. The first item will be mapped to 1, the second item to
            2, etc.
        """
        category_map = {}
        for i, item in enumerate(enum_items):
            category_map[item] = i + 1
        super(EnumColumn, self).__init__(category_map)


class BitmappedSetColumn(ColumnType):

    def __init__(self, set_items):
        """
        set_items:
            Sequence of strings that will be set members. Order is
            important! If you change the order of any item in the list, that
            will change the bitmap representation.
        """
        set_spec = {}
        for i, item in enumerate(set_items):
            set_spec[item] = 2**i
        self.set_spec = set_spec

    def __call__(self, value):
        """
        value is expected to be a string containing a list of set items
        separated by semicolons, potentially with surrounding spaces to be
        stripped off.

        Example value::

            "Assembly; C#; Clojure; Dart; Elixir; Erlang; F#"

        Return the bitmapped set representation as an integer.
        Ignore any items that don't exist in the set specification.
        If value is an empty string, return None.
        """
        if not value:
            return None
        items = [item.strip() for item in value.split(';')]
        result = 0
        for item in items:
            result |= self.set_spec.get(item, 0)
        return result

    def to_str(self, encoded_value):
        """Attempt to re-create the original string from an encoded value."""
        result = []
        for item, bitmask in self.set_spec.iteritems():
            if bitmask & encoded_value:
                result.append((bitmask, item))
        result.sort()
        return '; '.join(item[1] for item in result)


class FloatColumn(ColumnType):

    def __call__(self, value):
        try:
            return float(value)
        except ValueError:
            return None

FLOAT_COLUMN = FloatColumn()


class IntColumn(ColumnType):

    def __call__(self, value):
        try:
            return int(value)
        except ValueError:
            return None

INT_COLUMN = IntColumn()


class StrColumn(ColumnType):

    def __call__(self, value):
        if value:
            return value
        return None

STR_COLUMN = StrColumn()


CATEGORY_AGREEMENT = CategoryColumn({
    'Strongly agree': 2,
    'Agree': 1,
    'Somewhat agree': 0,
    'Disagree': -1,
    'Strongly disagree': -2,
})

CATEGORY_IMPORTANCE = CategoryColumn({
    'Very important': 4,
    'Important': 3,
    'Somewhat important': 2,
    'Not very important': 1,
    'Not at all important': 0,
})

CATEGORY_YESNO = CategoryColumn({
    'Yes': True,
    'No': False,
})

CATEGORY_GENDER = CategoryColumn({
    'Female': 1,
    'Male': 2,
    'Non-Conforming': 3,
    'Transgender': 4,
})

CATEGORY_BOOLEAN = CategoryColumn({
    'False': False,
    'True': True,
})

CATEGORY_SATISFACTION = CategoryColumn({
    'Not at all satisfied': 0,
    'Not very satisfied': 1,
    'Somewhat satisfied': 2,
    'Satisfied': 3,
    'Very satisfied': 4,
})

CATEGORY_INFLUENCE = CategoryColumn({
    'No influence at all': 0,
    'Not much influence': 1,
    'Some influence': 2,
    'A lot of influence': 3,
    'I am the final decision maker': 4,
})

CATEGORY_FREQUENCY = CategoryColumn({
    'At least once each day': 4,
    'At least once each week': 3,
    'Several times': 2,
    'Once or twice': 1,
    "Haven't done it at all": 0,
})

CATEGORY_STACKOVERFLOW_DESCRIBES = CategoryColumn({
    "I've visited stack overflow, but haven't logged in/created an account": 1,
    "I have a login for stack overflow, but haven't created a CV or developer story": 2,
    'I have created a CV or developer story on stack overflow': 3,
})

CATEGORY_MAJOR_UNDERGRAD = CategoryColumn({
    'A business discipline': 1,
    'A health science': 2,
    'A humanities discipline': 3,
    'A natural science': 4,
    'A non-computer-focused engineering discipline': 5,
    'A social science': 6,
    'Computer engineering or electrical/electronics engineering': 7,
    'Computer programming or web development': 8,
    'Computer science or software engineering': 9,
    'Fine arts or performing arts': 10,
    'I never declared a major': 11,
    'Information technology, networking, or system administration': 12,
    'Management information systems': 13,
    'Mathematics or statistics': 14,
    'Psychology': 15,
    'Something else': 16,
})

CATEGORY_JOB_SEEKING_STATUS = CategoryColumn({
    "I am not interested in new job opportunities": 0,
    "I'm not actively looking, but I am open to new opportunities": 1,
    "I am actively looking for a job": 2,
})

CATEGORY_COMPANY_TYPE = CategoryColumn({
    "Government agency or public school/university": 1,
    "I don't know": 2,
    "I prefer not to answer": 3,
    "Non-profit/non-governmental organization or private school/university": 4,
})

CATEGORY_PRONOUNCE_GIF = CategoryColumn({
    "Enunciating each letter: \"gee eye eff\"": 1,
    "Some other way": 2,
    "With a hard \"g,\" like \"gift\"": 3,
    "With a soft \"g,\" like \"jiff\"": 4,
})

CATEGORY_FORMAL_EDUCATION = CategoryColumn({
    "I prefer not to answer": 0,
    "I never completed any formal education": 1,
    "Primary/elementary school": 2,
    "Secondary school": 3,
    "Some college/university study without earning a bachelor's degree": 4,
    "Bachelor's degree": 5,
    "Master's degree": 6,
    "Doctoral degree": 7,
    "Professional degree": 8,
})

CATEGORY_EMPLOYMENT_STATUS = CategoryColumn({
    "Employed full-time": 1,
    "Employed part-time": 2,
    "Independent contractor, freelancer, or self-employed": 3,
    "Not employed, and not looking for work": 4,
    "Not employed, but looking for work": 5,
    "Retired": 6,
})

CATEGORY_PROFESSIONAL = CategoryColumn({
    "Professional developer": 1,
    "Professional non-developer who sometimes writes code": 2,
    "Student": 3,
    "Used to be a professional developer": 4,
})

CATEGORY_LEARNED_HIRING = CategoryColumn({
    "A career fair or on-campus recruiting event": 1,
    "A friend, family member, or former colleague told me": 2,
    "A general-purpose job board": 3,
    "A tech-specific job board": 4,
    "An external recruiter or headhunter": 5,
    "I visited the company's Web site and found a job listing there": 6,
    "I was contacted directly by someone at the company (e.g. internal recruiter)": 7,
    "Some other way": 8,
})

CATEGORY_HOME_REMOTE = CategoryColumn({
    "A few days each month": 1,
    "About half the time": 2,
    "All or almost all the time (I'm full-time remote)": 3,
    "It's complicated": 4,
    "Less than half the time, but at least one day each week": 5,
    "More than half, but not all, the time": 6,
    "Never": 7,
})

CATEGORY_PROGRAM_HOBBY = CategoryColumn({
    "No": 1,
    "Yes, I contribute to open source projects": 2,
    "Yes, I program as a hobby": 3,
    "Yes, both": 4,
})

CATEGORY_AUDITORY_ENVIRONMENT = CategoryColumn({
    "Keep the room absolutely quiet": 1,
    "Put on a movie or TV show": 2,
    "Put on some ambient sounds (e.g. whale songs, forest sounds)": 3,
    "Something else": 4,
    "Turn on some music": 5,
    "Turn on the news or talk radio": 6,
})

CATEGORY_COMPANY_SIZE = CategoryColumn({
    "1,000 to 4,999 employees": 1,
    "10 to 19 employees": 2,
    "10,000 or more employees": 3,
    "100 to 499 employees": 4,
    "20 to 99 employees": 5,
    "5,000 to 9,999 employees": 6,
    "500 to 999 employees": 7,
    "Fewer than 10 employees": 8,
})

CATEGORY_RESUME_PROMPTED = CategoryColumn({
    "A friend told me about a job opportunity": 1,
    "A recruiter contacted me": 2,
    "I completed a major project, assignment, or contract": 3,
    "I received bad news about the future of my company or depart": 4,
    "I received negative feedback on my job performance": 5,
    "I saw an employer's advertisement": 6,
    "I was just giving it a regular update": 7,
    "Something else": 8,
})

CATEGORY_HIGHEST_EDUCATION = CategoryColumn({
    "A bachelor's degree": 1,
    "A doctoral degree": 2,
    "A master's degree": 3,
    "A professional degree": 4,
    "High school": 5,
    "I don't know/not sure": 6,
    "I prefer not to answer": 7,
    "No education": 8,
    "Primary/elementary school": 9,
    "Some college/university study, no bachelor's degree": 10,
})

CATEGORY_LAST_NEW_JOB = CategoryColumn({
    "Between 1 and 2 years ago": 1,
    "Between 2 and 4 years ago": 2,
    "Less than a year ago": 3,
    "More than 4 years ago": 4,
    "Not applicable/ never": 5,
})

CATEGORY_YEARS_CODED = CategoryColumn({
    "1 to 2 years": 1,
    "10 to 11 years": 2,
    "11 to 12 years": 3,
    "12 to 13 years": 4,
    "13 to 14 years": 5,
    "14 to 15 years": 6,
    "15 to 16 years": 7,
    "16 to 17 years": 8,
    "17 to 18 years": 9,
    "18 to 19 years": 10,
    "19 to 20 years": 11,
    "2 to 3 years": 12,
    "20 or more years": 13,
    "3 to 4 years": 14,
    "4 to 5 years": 15,
    "5 to 6 years": 16,
    "6 to 7 years": 17,
    "7 to 8 years": 18,
    "8 to 9 years": 19,
    "9 to 10 years": 20,
    "Less than a year": 21,
})

CATEGORY_CODE_CHECKIN_FREQUENCY = CategoryColumn({
    "A few times a month": 1,
    "A few times a week": 2,
    "Just a few times over the year": 3,
    "Multiple times a day": 4,
    "Never": 5,
    "Once a day": 6,
})

CATEGORY_UNIVERSITY = CategoryColumn({
    "No": 1,
    "Yes, full-time": 2,
    "Yes, part-time": 3,
})

CATEGORY_OVERPAID = CategoryColumn({
    "Greatly overpaid": 1,
    "Greatly underpaid": 2,
    "Neither underpaid nor overpaid": 3,
    "Somewhat overpaid": 4,
    "Somewhat underpaid": 5,
})

SET_PROGRAMMING_LANG = BitmappedSetColumn([
  "Assembly",
  "C",
  "C#",
  "C++",
  "Clojure",
  "CoffeeScript",
  "Common Lisp",
  "Dart",
  "Elixir",
  "Erlang",
  "F#",
  "Go",
  "Groovy",
  "Hack",
  "Haskell",
  "Java",
  "JavaScript",
  "Julia",
  "Lua",
  "Matlab",
  "NA",
  "Objective-C",
  "PHP",
  "Perl",
  "Python",
  "R",
  "Ruby",
  "Rust",
  "SQL",
  "Scala",
  "Smalltalk",
  "Swift",
  "TypeScript",
  "VB.NET",
  "VBA",
  "Visual Basic 6",
])

ENUM_COUNTRY = EnumColumn(COUNTRIES)

ENUM_CURRENCY = EnumColumn([
    # Unicode currency characters are encoding using UTF-8
    "Australian dollars (A$)",
    "Bitcoin (btc)",
    "Brazilian reais (R$)",
    "British pounds sterling (\xc2\xa3)",  # U+00A3
    "Canadian dollars (C$)",
    "Chinese yuan renminbi (\xc2\xa5)",  # U+00A5 (exactly as in the file)
    "Euros (\xe2\x82\xac)",  # U+20AC
    "Indian rupees (?)",
    "Japanese yen (\xc2\xa5)",  # U+00A5
    "Mexican pesos (MXN$)",
    "Polish zloty (zl)",
    "Russian rubles (?)",
    "Singapore dollars (S$)",
    "South African rands (R)",
    "Swedish kroner (SEK)",
    "Swiss francs",
    "U.S. dollars ($)",
])

ENUM_WEB_DEVELOPER_TYPE = EnumColumn([
    "Back-end Web developer",
    "Front-end Web developer",
    "Full stack Web developer",
])

ENUM_WORK_START = EnumColumn([
    "10:00 AM",
    "10:00 PM",
    "11:00 AM",
    "11:00 PM",
    "1:00 AM",
    "1:00 PM",
    "2:00 AM",
    "2:00 PM",
    "3:00 AM",
    "3:00 PM",
    "4:00 AM",
    "4:00 PM",
    "5:00 AM",
    "5:00 PM",
    "6:00 AM",
    "6:00 PM",
    "7:00 AM",
    "7:00 PM",
    "8:00 AM",
    "8:00 PM",
    "9:00 AM",
    "9:00 PM",
    "Midnight",
    "Noon",
])

ENUM_VERSION_CONTROL = EnumColumn([
    "Copying and pasting files to network shares",
    "Git",
    "I don't use version control",
    "I use some other system",
    "Mercurial",
    "Rational ClearCase",
    "Subversion",
    "Team Foundation Server",
    "Visual Source Safe",
    "Zip file back-ups",
])

ENUM_TABS_SPACES = EnumColumn([
    "Both",
    "Spaces",
    "Tabs",
])

ENUM_TIME_AFTER_BOOTCAMP = EnumColumn([
    "Four to six months",
    "I already had a job as a developer when I started the program",
    "I got a job as a developer before completing the program",
    "I haven't gotten a job as a developer yet",
    "Immediately upon graduating",
    "Less than a month",
    "Longer than a year",
    "One to three months",
    "Six months to a year",
])


# Column rules list for generating column type info from header name.
# These are obviously tuned for a certain corpus of test data!
# List of tuples:
#
# [ (header_re_pattern, column_type_instance), ... ]

COLUMN_RULES = [
    (r"[A-Za-z]+-\d+$",             CATEGORY_BOOLEAN),
    (r"AnnoyingUI$",                CATEGORY_AGREEMENT),
    (r"Assess.*",                   CATEGORY_IMPORTANCE),
    (r"AuditoryEnvironment$",       CATEGORY_AUDITORY_ENVIRONMENT),
    (r"BoringDetails$",             CATEGORY_AGREEMENT),
    (r"BuildingThings$",            CATEGORY_AGREEMENT),
    (r"CareerSatisfaction$",        FLOAT_COLUMN),
    (r"ChallengeMyself$",           CATEGORY_AGREEMENT),
    (r"ChangeWorld$",               CATEGORY_AGREEMENT),
    (r"CheckInCode$",               CATEGORY_CODE_CHECKIN_FREQUENCY),
    (r"ClickyKeys$",                CATEGORY_YESNO),
    (r"CollaborateRemote$",         CATEGORY_AGREEMENT),
    (r"Combined Gender$",           CATEGORY_GENDER),
    (r"CompanySize$",               CATEGORY_COMPANY_SIZE),
    (r"CompanyType$",               CATEGORY_COMPANY_TYPE),
    (r"CompetePeers$",              CATEGORY_AGREEMENT),
    (r"Country$",                   ENUM_COUNTRY),
    (r"Currency$",                  ENUM_CURRENCY),
    (r"DifficultCommunication$",    CATEGORY_AGREEMENT),
    (r"EmploymentStatus$",          CATEGORY_EMPLOYMENT_STATUS),
    (r"EnjoyDebugging$",            CATEGORY_AGREEMENT),
    (r".*Satisfied",                CATEGORY_SATISFACTION),
    (r"ExCoder",                    CATEGORY_AGREEMENT),
    (r"ExpectedSalary",             FLOAT_COLUMN),
    (r"FormalEducation$",           CATEGORY_FORMAL_EDUCATION),
    (r"FriendsDevelopers$",         CATEGORY_AGREEMENT),
    (r"HighestEducationParents$",   CATEGORY_HIGHEST_EDUCATION),
    (r"HomeRemote$",                CATEGORY_HOME_REMOTE),
    (r"HoursPerWeek$",              INT_COLUMN),
    (r"Important.*",                CATEGORY_IMPORTANCE),
    (r"InTheZone$",                 CATEGORY_AGREEMENT),
    # InfluenceInternet is a little weird, answers show satisfaction not
    # influence.
    (r"InfluenceInternet$",         CATEGORY_SATISFACTION),
    (r"Influence",                  CATEGORY_INFLUENCE),
    (r"InterestedAnswers$",         CATEGORY_AGREEMENT),
    (r"InvestTimeTools$",           CATEGORY_AGREEMENT),
    (r"JobSatisfaction$",           FLOAT_COLUMN),
    (r"JobSecurity$",               CATEGORY_AGREEMENT),
    (r"JobSeekingStatus$",          CATEGORY_JOB_SEEKING_STATUS),
    (r"KinshipDevelopers$",         CATEGORY_AGREEMENT),
    (r"LastNewJob$",                CATEGORY_LAST_NEW_JOB),
    (r"LearnedHiring$",             CATEGORY_LEARNED_HIRING),
    (r"LearningNewTech$",           CATEGORY_AGREEMENT),
    (r"MajorUndergrad$",            CATEGORY_MAJOR_UNDERGRAD),
    (r"OtherPeoplesCode$",          CATEGORY_AGREEMENT),
    (r"Overpaid$",                  CATEGORY_OVERPAID),
    (r"ProblemSolving$",            CATEGORY_AGREEMENT),
    (r"Professional$",              CATEGORY_PROFESSIONAL),
    (r"ProgramHobby$",              CATEGORY_PROGRAM_HOBBY),
    (r"ProjectManagement$",         CATEGORY_AGREEMENT),
    (r"PronounceGIF$",              CATEGORY_PRONOUNCE_GIF),
    (r"QuestionsConfusing$",        CATEGORY_AGREEMENT),
    (r"QuestionsInteresting$",      CATEGORY_AGREEMENT),
    (r"ResumePrompted$",            CATEGORY_RESUME_PROMPTED),
    (r"RightWrongWay$",             CATEGORY_AGREEMENT),
    (r"Salary$",                    FLOAT_COLUMN),
    (r"SalaryAdjusted$",            FLOAT_COLUMN),
    (r"SeriousWork$",               CATEGORY_AGREEMENT),
    (r"ShipIt$",                    CATEGORY_AGREEMENT),
    (r"StackOverflowAdsDistracting$", CATEGORY_AGREEMENT),
    (r"StackOverflowAdsRelevant$",  CATEGORY_AGREEMENT),
    (r"StackOverflowAnswer$",       CATEGORY_FREQUENCY),
    (r"StackOverflowBetter$",       CATEGORY_AGREEMENT),
    (r"StackOverflowCommunity$",    CATEGORY_AGREEMENT),
    (r"StackOverflowCompanyPage$",  CATEGORY_FREQUENCY),
    (r"StackOverflowCopiedCode$",   CATEGORY_FREQUENCY),
    (r"StackOverflowDescribes$",    CATEGORY_STACKOVERFLOW_DESCRIBES),
    (r"StackOverflowFoundAnswer$",  CATEGORY_FREQUENCY),
    (r"StackOverflowHelpful$",      CATEGORY_AGREEMENT),
    (r"StackOverflowJobListing$",   CATEGORY_FREQUENCY),
    (r"StackOverflowJobSearch$",    CATEGORY_FREQUENCY),
    (r"StackOverflowMakeMoney$",    CATEGORY_AGREEMENT),
    (r"StackOverflowMetaChat$",     CATEGORY_FREQUENCY),
    (r"StackOverflowModeration$",   CATEGORY_AGREEMENT),
    (r"StackOverflowNewQuestion$",  CATEGORY_FREQUENCY),
    (r"StackOverflowSatisfaction$", FLOAT_COLUMN),
    (r"StackOverflowWhatDo$",       CATEGORY_AGREEMENT),
    (r"SurveyLong$",                CATEGORY_AGREEMENT),
    (r"TabsSpaces$",                ENUM_TABS_SPACES),
    (r"TimeAfterBootcamp$",         ENUM_TIME_AFTER_BOOTCAMP),
    (r"UnderstandComputers$",       CATEGORY_AGREEMENT),
    (r"University$",                CATEGORY_UNIVERSITY),
    (r"VersionControl$",            ENUM_VERSION_CONTROL),
    (r"WantWorkLanguage$",          SET_PROGRAMMING_LANG),
    (r"WebDeveloperType$",          ENUM_WEB_DEVELOPER_TYPE),
    (r"WorkPayCare$",               CATEGORY_AGREEMENT),
    (r"WorkStart$",                 ENUM_WORK_START),
    (r"YearsCoded.*$",              CATEGORY_YEARS_CODED),
    (r"YearsProgram$",              CATEGORY_YEARS_CODED),
    (r".*Important$",               CATEGORY_IMPORTANCE),
    # Catch all: Treat column value as string
    (r"",                           STR_COLUMN),
]


def get_column_type(header, column_rules=None):
    if column_rules is None:
        column_rules = COLUMN_RULES

    for pattern, column_type in column_rules:
        if re.match(pattern, header):
            return column_type
    raise Exception("No column type for header: {}".format(header))


def get_converter_funcs(headers, column_rules=None):
    return [get_column_type(header, column_rules=column_rules)
            for header in headers]
