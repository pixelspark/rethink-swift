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

public typealias ReDocument = [String: Any]

public class ReDatum: ReQueryValue {
	public let jsonSerialization: Any
	private static let reqlTypeTime = "TIME"
	private static let reqlTypeBinary = "BINARY"
	private static let reqlSpecialKey = "$reql_type$"

	internal init() {
		self.jsonSerialization = NSNull()
	}

	internal init(string: String) {
		self.jsonSerialization = string
	}

	internal init(double: Double) {
		self.jsonSerialization = double
	}

	internal init(int: Int) {
		self.jsonSerialization = int
	}

	internal init(bool: Bool) {
		self.jsonSerialization = bool
	}

	internal init(array: [ReQueryValue]) {
		self.jsonSerialization = [ReTerm.make_array.rawValue, array.map { return $0.jsonSerialization }]
	}

	internal init(document: ReDocument) {
		self.jsonSerialization = document
	}

	internal init(object: [String: ReQueryValue]) {
		var serialized: [String: Any] = [:]
		for (key, value) in object {
			serialized[key] = value.jsonSerialization
		}
		self.jsonSerialization = serialized
	}

	internal init(date: Date) {
		self.jsonSerialization = [ReDatum.reqlSpecialKey: ReDatum.reqlTypeTime, "epoch_time": date.timeIntervalSince1970, "timezone": "+00:00"]
	}

	internal init(data: Data) {
		self.jsonSerialization = [ReDatum.reqlSpecialKey: ReDatum.reqlTypeBinary, "data": data.base64EncodedString(options: [])]
	}

	internal init(jsonSerialization: Any) {
		self.jsonSerialization = jsonSerialization
	}

	internal var value: Any { get {
		if let d = self.jsonSerialization as? [String: Any], let t = d[ReDatum.reqlSpecialKey] as? String {
			if t == ReDatum.reqlTypeBinary {
				if let data = (self.jsonSerialization as AnyObject).value(forKey: "data") as? String {
					return Data(base64Encoded: data, options: [])!
				}
				else {
					fatalError("invalid binary datum received")
				}
			}
			else if t == ReDatum.reqlTypeTime {
				let epochTime = (self.jsonSerialization as AnyObject).value(forKey: "epoch_time") as AnyObject
				
				if let timezone = (self.jsonSerialization as AnyObject).value(forKey: "timezone") as? String {
					// TODO: interpret server timezone other than +00:00 (UTC)
					assert(timezone == "+00:00", "support for timezones other than UTC not implemented (yet)")
					return Date(timeIntervalSince1970: epochTime.doubleValue!)
				}
				else {
					fatalError("invalid date received")
				}
			}
			else {
				fatalError("unrecognized $reql_type$ in serialized data")
			}
		}
		else {
			return self.jsonSerialization
		}
	} }
}
