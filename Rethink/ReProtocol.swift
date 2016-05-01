/**  Rethink.swift
Copyright (c) 2015 Pixelspark
Author: Tommy van der Vorst (tommy@pixelspark.nl)

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE. **/
import Foundation

/** Protocol constants. */
internal class ReProtocol {
	static let defaultPort = 28015

	static let protocolType: UInt32 = 0x7e6970c7 // JSON
	static let handshakeSuccessResponse = "SUCCESS"
	static let defaultUser = "admin"
	static let defaultPassword = ""

	static let responseTypeSuccessAtom = 1
	static let responseTypeSuccessSequence = 2
	static let responseTypeSuccessPartial = 3
	static let responseTypeWaitComplete = 4
	static let responseTypeClientError  = 16
	static let responseTypeCompileError = 17
	static let responseTypeRuntimeError = 18

	enum ReQueryType: Int {
		case START =  1
		case CONTINUE = 2
		case STOP =  3
		case NOREPLY_WAIT = 4
	}
}

/** These constants can be found in the RethinkDB Java driver source code:
https://github.com/rethinkdb/rethinkdb/blob/next/drivers/java/term_info.json */
internal enum ReTerm: Int {
	case DATUM = 1
	case MAKE_ARRAY = 2
	case MAKE_OBJ = 3
	case VAR = 10
	case JAVASCRIPT = 11
	case UUID = 169
	case HTTP = 153
	case ERROR = 12
	case IMPLICIT_VAR = 13
	case DB = 14
	case TABLE = 15
	case GET = 16
	case GET_ALL = 78
	case EQ = 17
	case NE = 18
	case LT = 19
	case LE = 20
	case GT = 21
	case GE = 22
	case NOT = 23
	case ADD = 24
	case SUB = 25
	case MUL = 26
	case DIV = 27
	case MOD = 28
	case FLOOR = 183
	case CEIL = 184
	case ROUND = 185
	case APPEND = 29
	case PREPEND = 80
	case DIFFERENCE = 95
	case SET_INSERT = 88
	case SET_INTERSECTION = 89
	case SET_UNION = 90
	case SET_DIFFERENCE = 91
	case SLICE = 30
	case SKIP = 70
	case LIMIT = 71
	case OFFSETS_OF = 87
	case CONTAINS = 93
	case GET_FIELD = 31
	case KEYS = 94
	case OBJECT = 143
	case HAS_FIELDS = 32
	case WITH_FIELDS = 96
	case PLUCK = 33
	case WITHOUT = 34
	case MERGE = 35
	case BETWEEN_DEPRECATED = 36
	case BETWEEN = 182
	case REDUCE = 37
	case MAP = 38
	case FILTER = 39
	case CONCAT_MAP = 40
	case ORDER_BY = 41
	case DISTINCT = 42
	case COUNT = 43
	case IS_EMPTY = 86
	case UNION = 44
	case NTH = 45
	case BRACKET = 170
	case INNER_JOIN = 48
	case OUTER_JOIN = 49
	case EQ_JOIN = 50
	case ZIP = 72
	case RANGE = 173
	case INSERT_AT = 82
	case DELETE_AT = 83
	case CHANGE_AT = 84
	case SPLICE_AT = 85
	case COERCE_TO = 51
	case TYPE_OF = 52
	case UPDATE = 53
	case DELETE = 54
	case REPLACE = 55
	case INSERT = 56
	case DB_CREATE = 57
	case DB_DROP = 58
	case DB_LIST = 59
	case TABLE_CREATE = 60
	case TABLE_DROP = 61
	case TABLE_LIST = 62
	case CONFIG = 174
	case STATUS = 175
	case WAIT = 177
	case RECONFIGURE = 176
	case REBALANCE = 179
	case SYNC = 138
	case INDEX_CREATE = 75
	case INDEX_DROP = 76
	case INDEX_LIST = 77
	case INDEX_STATUS = 139
	case INDEX_WAIT = 140
	case INDEX_RENAME = 156
	case FUNCALL = 64
	case BRANCH = 65
	case OR = 66
	case AND = 67
	case FOR_EACH = 68
	case FUNC = 69
	case ASC = 73
	case DESC = 74
	case INFO = 79
	case MATCH = 97
	case UPCASE = 141
	case DOWNCASE = 142
	case SAMPLE = 81
	case DEFAULT = 92
	case JSON = 98
	case TO_JSON_STRING = 172
	case ISO8601 = 99
	case TO_ISO8601 = 100
	case EPOCH_TIME = 101
	case TO_EPOCH_TIME = 102
	case NOW = 103
	case IN_TIMEZONE = 104
	case DURING = 105
	case DATE = 106
	case TIME_OF_DAY = 126
	case TIMEZONE = 127
	case YEAR = 128
	case MONTH = 129
	case DAY = 130
	case DAY_OF_WEEK = 131
	case DAY_OF_YEAR = 132
	case HOURS = 133
	case MINUTES = 134
	case SECONDS = 135
	case TIME = 136
	case MONDAY = 107
	case TUESDAY = 108
	case WEDNESDAY = 109
	case THURSDAY = 110
	case FRIDAY = 111
	case SATURDAY = 112
	case SUNDAY = 113
	case JANUARY = 114
	case FEBRUARY = 115
	case MARCH = 116
	case APRIL = 117
	case MAY = 118
	case JUNE = 119
	case JULY = 120
	case AUGUST = 121
	case SEPTEMBER = 122
	case OCTOBER = 123
	case NOVEMBER = 124
	case DECEMBER = 125
	case LITERAL = 137
	case GROUP = 144
	case SUM = 145
	case AVG = 146
	case MIN = 147
	case MAX = 148
	case SPLIT = 149
	case UNGROUP = 150
	case RANDOM = 151
	case CHANGES = 152
	case ARGS = 154
	case BINARY = 155
	case GEOJSON = 157
	case TO_GEOJSON = 158
	case POINT = 159
	case LINE = 160
	case POLYGON = 161
	case DISTANCE = 162
	case INTERSECTS = 163
	case INCLUDES = 164
	case CIRCLE = 165
	case GET_INTERSECTING = 166
	case FILL = 167
	case GET_NEAREST = 168
	case POLYGON_SUB = 171
	case MINVAL = 180
	case MAXVAL = 181
	case FOLD = 187
	case GRANT = 188

}
