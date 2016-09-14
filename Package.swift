import PackageDescription

let package = Package(
	name: "Rethink",
	targets: [
		Target(name: "Rethink", dependencies: ["GCDAsyncSocket", "SCRAM"]),
		Target(name: "GCDAsyncSocket"),
		Target(name: "SCRAM")
	]
)
