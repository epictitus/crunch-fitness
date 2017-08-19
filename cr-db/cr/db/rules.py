"""
Implement rules for converting and normalizing data column values
"""
import re


# Column types encapsulate operations to convert str values from the CSV
# file to normalized values. I'm not just using straight converter functions
# because I want to be able to do a reverse lookup of a cateogry string from
# a normalized integer value.

class ColumnType(object):

    def __call__(self, value):
        """Convert column raw string value to normalized value"""
        raise NotImplementedError()


class CategoryColumn(ColumnType):

    def __init__(self, category_map=None):
        self.category_map = category_map

    def __call__(self, value):
        return self.category_map.get(value.lower())


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
    'strongly agree': 2,
    'agree': 1,
    'somewhat agree': 0,
    'disagree': -1,
})

CATEGORY_IMPORTANCE = CategoryColumn({
    'very important': 4,
    'important': 3,
    'somewhat important': 2,
    'not very important': 1,
    'not at all important': 0,
})

CATEGORY_YESNO = CategoryColumn({
    'yes': True,
    'no': False,
})

CATEGORY_GENDER = CategoryColumn({
    'female': 1,
    'male': 2,
    'non-conforming': 3,
    'transgender': 4,
})

CATEGORY_BOOLEAN = CategoryColumn({
    'false': False,
    'true': True,
})

CATEGORY_SATISFACTION = CategoryColumn({
    'not at all satisfied': 0,
    'not very satisfied': 1,
    'somewhat satisfied': 2,
    'satisfied': 3,
    'very satisfied': 4,
})

CATEGORY_INFLUENCE = CategoryColumn({
    'no influence at all': 0,
    'not much influence': 1,
    'some influence': 2,
    'a lot of influence': 3,
    'i am the final decision maker': 4,
})

CATEGORY_FREQUENCY = CategoryColumn({
    'at least once each day': 4,
    'at least once each week': 3,
    'several times': 2,
    'once or twice': 1,
    "haven't done it at all": 0,
})


# Column rules list for generating column type info from header name.
# These are obviously tuned for a certain corpus of test data!
# List of tuples:
#
# [ (header_re_pattern, column_type_instance), ... ]

COLUMN_RULES = [
    (r"[A-Za-z]+-\d+$",             CATEGORY_BOOLEAN),
    (r"AnnoyingUI$",                CATEGORY_AGREEMENT),
    (r"Assess",                     CATEGORY_IMPORTANCE),
    (r"BoringDetails$",             CATEGORY_AGREEMENT),
    (r"BuildingThings$",            CATEGORY_AGREEMENT),
    (r"CareerSatisfaction$",        FLOAT_COLUMN),
    (r"ChallengeMyself$",           CATEGORY_AGREEMENT),
    (r"ChangeWorld$",               CATEGORY_AGREEMENT),
    (r"ClickyKeys$",                CATEGORY_YESNO),
    (r"CollaborateRemote$",         CATEGORY_AGREEMENT),
    (r"Combined Gender$",           CATEGORY_GENDER),
    (r"CompetePeers$",              CATEGORY_AGREEMENT),
    (r"DifficultCommunication$",    CATEGORY_AGREEMENT),
    (r".*Important$",               CATEGORY_IMPORTANCE),
    (r"EnjoyDebugging$",            CATEGORY_AGREEMENT),
    (r".*Satisfied",                CATEGORY_SATISFACTION),
    (r"ExCoder",                    CATEGORY_AGREEMENT),
    (r"ExpectedSalary",             FLOAT_COLUMN),
    (r"FriendsDevelopers$",         CATEGORY_AGREEMENT),
    (r"HoursPerWeek$",              INT_COLUMN),
    (r"InTheZone$",                 CATEGORY_AGREEMENT),
    # InfluenceInternet is a little weird, answers show satisfaction not
    # influence.
    (r"InfluenceInternet$",         CATEGORY_SATISFACTION),
    (r"Influence",                  CATEGORY_INFLUENCE),
    (r"InterestedAnswers$",         CATEGORY_AGREEMENT),
    (r"InvestTimeTools$",           CATEGORY_AGREEMENT),
    (r"JobSatisfaction$",           FLOAT_COLUMN),
    (r"JobSecurity$",               CATEGORY_AGREEMENT),
    (r"KinshipDevelopers$",         CATEGORY_AGREEMENT),
    (r"LearningNewTeach$",          CATEGORY_AGREEMENT),
    (r"OtherPeoplesCode$",          CATEGORY_AGREEMENT),
    (r"ProblemSolving$",            CATEGORY_AGREEMENT),
    (r"ProjectManagement$",         CATEGORY_AGREEMENT),
    (r"QuestionsConfusing$",        CATEGORY_AGREEMENT),
    (r"QuestionsInteresting$",      CATEGORY_AGREEMENT),
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
    (r"UnderstandComputers$",       CATEGORY_AGREEMENT),
    (r"WorkPayCare$",               CATEGORY_AGREEMENT),
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
