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
		case start =  1
		case `continue` = 2
		case stop =  3
		case noreply_WAIT = 4
	}
}

/** These constants can be found in the RethinkDB Java driver source code:
https://github.com/rethinkdb/rethinkdb/blob/next/drivers/java/term_info.json */
internal enum ReTerm: Int {
	case datum = 1
	case make_ARRAY = 2
	case make_OBJ = 3
	case `var` = 10
	case javascript = 11
	case uuid = 169
	case http = 153
	case error = 12
	case implicit_VAR = 13
	case db = 14
	case table = 15
	case get = 16
	case get_ALL = 78
	case eq = 17
	case ne = 18
	case lt = 19
	case le = 20
	case gt = 21
	case ge = 22
	case not = 23
	case add = 24
	case sub = 25
	case mul = 26
	case div = 27
	case mod = 28
	case floor = 183
	case ceil = 184
	case round = 185
	case append = 29
	case prepend = 80
	case difference = 95
	case set_INSERT = 88
	case set_INTERSECTION = 89
	case set_UNION = 90
	case set_DIFFERENCE = 91
	case slice = 30
	case skip = 70
	case limit = 71
	case offsets_OF = 87
	case contains = 93
	case get_FIELD = 31
	case keys = 94
	case object = 143
	case has_FIELDS = 32
	case with_FIELDS = 96
	case pluck = 33
	case without = 34
	case merge = 35
	case between_DEPRECATED = 36
	case between = 182
	case reduce = 37
	case map = 38
	case filter = 39
	case concat_MAP = 40
	case order_BY = 41
	case distinct = 42
	case count = 43
	case is_EMPTY = 86
	case union = 44
	case nth = 45
	case bracket = 170
	case inner_JOIN = 48
	case outer_JOIN = 49
	case eq_JOIN = 50
	case zip = 72
	case range = 173
	case insert_AT = 82
	case delete_AT = 83
	case change_AT = 84
	case splice_AT = 85
	case coerce_TO = 51
	case type_OF = 52
	case update = 53
	case delete = 54
	case replace = 55
	case insert = 56
	case db_CREATE = 57
	case db_DROP = 58
	case db_LIST = 59
	case table_CREATE = 60
	case table_DROP = 61
	case table_LIST = 62
	case config = 174
	case status = 175
	case wait = 177
	case reconfigure = 176
	case rebalance = 179
	case sync = 138
	case index_CREATE = 75
	case index_DROP = 76
	case index_LIST = 77
	case index_STATUS = 139
	case index_WAIT = 140
	case index_RENAME = 156
	case funcall = 64
	case branch = 65
	case or = 66
	case and = 67
	case for_EACH = 68
	case `func` = 69
	case asc = 73
	case desc = 74
	case info = 79
	case match = 97
	case upcase = 141
	case downcase = 142
	case sample = 81
	case `default` = 92
	case json = 98
	case to_JSON_STRING = 172
	case iso8601 = 99
	case to_ISO8601 = 100
	case epoch_TIME = 101
	case to_EPOCH_TIME = 102
	case now = 103
	case in_TIMEZONE = 104
	case during = 105
	case date = 106
	case time_OF_DAY = 126
	case timezone = 127
	case year = 128
	case month = 129
	case day = 130
	case day_OF_WEEK = 131
	case day_OF_YEAR = 132
	case hours = 133
	case minutes = 134
	case seconds = 135
	case time = 136
	case monday = 107
	case tuesday = 108
	case wednesday = 109
	case thursday = 110
	case friday = 111
	case saturday = 112
	case sunday = 113
	case january = 114
	case february = 115
	case march = 116
	case april = 117
	case may = 118
	case june = 119
	case july = 120
	case august = 121
	case september = 122
	case october = 123
	case november = 124
	case december = 125
	case literal = 137
	case group = 144
	case sum = 145
	case avg = 146
	case min = 147
	case max = 148
	case split = 149
	case ungroup = 150
	case random = 151
	case changes = 152
	case args = 154
	case binary = 155
	case geojson = 157
	case to_GEOJSON = 158
	case point = 159
	case line = 160
	case polygon = 161
	case distance = 162
	case intersects = 163
	case includes = 164
	case circle = 165
	case get_INTERSECTING = 166
	case fill = 167
	case get_NEAREST = 168
	case polygon_SUB = 171
	case minval = 180
	case maxval = 181
	case fold = 187
	case grant = 188

}
