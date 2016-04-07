import Foundation

public enum ReTableReadMode: String {
	case Single = "single"
	case Majority = "majority"
	case Outdated = "outdated"
}

public enum ReTableIdentifierFormat: String {
	case Name = "name"
	case UUID = "uuid"
}

/** Optional arguments are instances of ReArg. */
public protocol ReArg {
	var serialization: (String, AnyObject) { get }
}

/** Optional arguments for the R.table command. */
public enum ReTableArg: ReArg {
	case ReadMode(ReTableReadMode)
	case IdentifierFormat(ReTableIdentifierFormat)

	public var serialization: (String, AnyObject) {
		switch self {
		case .ReadMode(let rm): return ("read_mode", rm.rawValue)
		case .IdentifierFormat(let i): return ("identifier_format", i.rawValue)
		}
	}
}

public enum ReFilterArg: ReArg {
	case Default(AnyObject)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Default(let a): return ("default", a)
		}
	}
}

public enum ReTableDurability: String {
	case Soft = "soft"
	case Hard = "hard"
}

public enum ReTableCreateArg: ReArg {
	case PrimaryKey(String)
	case Durability(ReTableDurability)
	case Shards(Int)
	case Replicas(Int)

	public var serialization: (String, AnyObject) {
		switch self {
		case .PrimaryKey(let p): return ("primary_key", p)
		case .Durability(let d): return ("durability", d.rawValue)
		case .Shards(let s): return ("shards", s)
		case .Replicas(let r): return ("replicas", r)
		}
	}
}

public enum RePermission: ReArg {
	case Read(Bool)
	case Write(Bool)
	case Connect(Bool)
	case Config(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Read(let b): return ("read", b)
		case .Write(let b): return ("write", b)
		case .Connect(let b): return ("connect", b)
		case .Config(let b): return ("config", b)
		}
	}
}

public enum ReChangesArg: ReArg {
	/** squash: Controls how change notifications are batched. Acceptable values are true, false and a numeric value:
	- true: When multiple changes to the same document occur before a batch of notifications is sent, the changes are 
	  "squashed" into one change. The client receives a notification that will bring it fully up to date with the server.
	- false: All changes will be sent to the client verbatim. This is the default.
	- n: A numeric value (floating point). Similar to true, but the server will wait n seconds to respond in order to 
	  squash as many changes together as possible, reducing network traffic. The first batch will always be returned 
	  immediately. */
	case Squash(Bool, n: Double?)

	/** changefeedQueueSize: the number of changes the server will buffer between client reads before it starts dropping 
	changes and generates an error (default: 100,000). */
	case ChangeFeedQueueSize(Int)

	/** includeInitial: if true, the changefeed stream will begin with the current contents of the table or selection 
	being monitored. These initial results will have new_val fields, but no old_val fields. The initial results may be 
	intermixed with actual changes, as long as an initial result for the changed document has already been given. If an 
	initial result for a document has been sent and a change is made to that document that would move it to the unsent 
	part of the result set (e.g., a changefeed monitors the top 100 posters, the first 50 have been sent, and poster 48 
	has become poster 52), an “uninitial” notification will be sent, with an old_val field but no new_val field.*/
	case IncludeInitial(Bool)

	/** includeStates: if true, the changefeed stream will include special status documents consisting of the field state
	and a string indicating a change in the feed’s state. These documents can occur at any point in the feed between the 
	notification documents described below. If includeStates is false (the default), the status documents will not be sent.*/
	case IncludeStates(Bool)

	/** includeOffsets: if true, a changefeed stream on an orderBy.limit changefeed will include old_offset and new_offset 
	fields in status documents that include old_val and new_val. This allows applications to maintain ordered lists of the
	stream’s result set. If old_offset is set and not null, the element at old_offset is being deleted; if new_offset is 
	set and not null, then new_val is being inserted at new_offset. Setting includeOffsets to true on a changefeed that 
	does not support it will raise an error.*/
	case IncludeOffsets(Bool)

	/** includeTypes: if true, every result on a changefeed will include a type field with a string that indicates the 
	kind of change the result represents: add, remove, change, initial, uninitial, state. Defaults to false.*/
	case IncludeTypes(Bool)

	public var serialization: (String, AnyObject) {
		switch self {
		case .Squash(let b, let i):
			assert(!(i != nil && !b), "Do not specify a time interval when squashing is to be disabled")
			if let interval = i where b {
				 return ("squash", interval)
			}
			else {
				return ("squash", b)
			}

		case .ChangeFeedQueueSize(let i): return ("changefeed_queue_size", i)
		case .IncludeInitial(let b): return ("include_initial", b)
		case .IncludeStates(let b): return ("include_states", b)
		case .IncludeOffsets(let b): return ("include_offsets", b)
		case .IncludeTypes(let b): return ("include_types", b)
		}
	}


}