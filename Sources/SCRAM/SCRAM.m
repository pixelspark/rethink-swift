/** SCRAM-SHA-256 implementation (RFC5802 http://tools.ietf.org/html/rfc5802).
 dapted from XMPPSCRAMSHA1Authentication.m (iPhoneXMPP), created by David Chiles on 3/21/14. **/

#import "SCRAM.h"
#import <CommonCrypto/CommonKeyDerivation.h>

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

@implementation NSData (SCRAM)

static char encodingTable[64] = {
	'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P',
	'Q','R','S','T','U','V','W','X','Y','Z','a','b','c','d','e','f',
	'g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v',
	'w','x','y','z','0','1','2','3','4','5','6','7','8','9','+','/' };

- (NSData*) xorWithData:(NSData*)data2 {
	NSMutableData *result = self.mutableCopy;

	char *dataPtr = (char *)result.mutableBytes;

	char *keyData = (char *)data2.bytes;

	char *keyPtr = keyData;
	int keyIndex = 0;

	for (int x = 0; x < self.length; x++) {
		*dataPtr = *dataPtr ^ *keyPtr;
		dataPtr++;
		keyPtr++;

		if (++keyIndex == data2.length) {
			keyIndex = 0;
			keyPtr = keyData;
		}
	}
	return result;
}

- (NSData*) hashWithAlgorithm:(CCHmacAlgorithm) algorithm key:(NSData*) key {
	unsigned char cHMAC[CC_SHA256_DIGEST_LENGTH];
	CCHmac(algorithm, [key bytes], [key length], [self bytes], [self length], cHMAC);
	return [[NSData alloc] initWithBytes:cHMAC length:sizeof(cHMAC)];
}

- (NSData*) hashPasswordWithAlgorithm:(CCHmacAlgorithm) algorithm salt:(NSData *)saltData iterations:(NSUInteger)rounds {
	NSMutableData *mutableSaltData = [saltData mutableCopy];
	UInt8 zeroHex = 0x00;
	UInt8 oneHex = 0x01;
	NSData *zeroData = [[NSData alloc] initWithBytes:&zeroHex length:sizeof(zeroHex)];
	NSData *oneData = [[NSData alloc] initWithBytes:&oneHex length:sizeof(oneHex)];

	[mutableSaltData appendData:zeroData];
	[mutableSaltData appendData:zeroData];
	[mutableSaltData appendData:zeroData];
	[mutableSaltData appendData:oneData];

	NSData *result = [mutableSaltData hashWithAlgorithm:algorithm key:self];
	NSData *previous = [result copy];

	for (int i = 1; i < rounds; i++) {
		previous = [previous hashWithAlgorithm:algorithm key:self];
		result = [result xorWithData:previous];
	}

	return result;
}

- (NSData*) sha256Digest {
	unsigned char result[CC_SHA256_DIGEST_LENGTH];
	CC_SHA256([self bytes], (CC_LONG)[self length], result);
	return [NSData dataWithBytes:result length:CC_SHA256_DIGEST_LENGTH];
}

- (NSString*) base64Encoded
{
	const unsigned char	*bytes = [self bytes];
	NSMutableString *result = [NSMutableString stringWithCapacity:[self length]];
	unsigned long ixtext = 0;
	unsigned long lentext = [self length];
	long ctremaining = 0;
	unsigned char inbuf[3], outbuf[4];
	unsigned short i = 0;
	unsigned short charsonline = 0, ctcopy = 0;
	unsigned long ix = 0;

	while( YES )
	{
		ctremaining = lentext - ixtext;
		if( ctremaining <= 0 ) break;

		for( i = 0; i < 3; i++ ) {
			ix = ixtext + i;
			if( ix < lentext ) inbuf[i] = bytes[ix];
			else inbuf [i] = 0;
		}

		outbuf [0] = (inbuf [0] & 0xFC) >> 2;
		outbuf [1] = ((inbuf [0] & 0x03) << 4) | ((inbuf [1] & 0xF0) >> 4);
		outbuf [2] = ((inbuf [1] & 0x0F) << 2) | ((inbuf [2] & 0xC0) >> 6);
		outbuf [3] = inbuf [2] & 0x3F;
		ctcopy = 4;

		switch( ctremaining )
		{
			case 1:
				ctcopy = 2;
				break;
			case 2:
				ctcopy = 3;
				break;
		}

		for( i = 0; i < ctcopy; i++ )
			[result appendFormat:@"%c", encodingTable[outbuf[i]]];

		for( i = ctcopy; i < 4; i++ )
			[result appendString:@"="];

		ixtext += 3;
		charsonline += 4;
	}

	return [NSString stringWithString:result];
}

- (NSData*) base64Decoded
{
	const unsigned char	*bytes = [self bytes];
	NSMutableData *result = [NSMutableData dataWithCapacity:[self length]];

	unsigned long ixtext = 0;
	unsigned long lentext = [self length];
	unsigned char ch = 0;
	unsigned char inbuf[4] = {0, 0, 0, 0};
	unsigned char outbuf[3] = {0, 0, 0};
	short i = 0, ixinbuf = 0;
	BOOL flignore = NO;
	BOOL flendtext = NO;

	while( YES )
	{
		if( ixtext >= lentext ) break;
		ch = bytes[ixtext++];
		flignore = NO;

		if( ( ch >= 'A' ) && ( ch <= 'Z' ) ) ch = ch - 'A';
		else if( ( ch >= 'a' ) && ( ch <= 'z' ) ) ch = ch - 'a' + 26;
		else if( ( ch >= '0' ) && ( ch <= '9' ) ) ch = ch - '0' + 52;
		else if( ch == '+' ) ch = 62;
		else if( ch == '=' ) flendtext = YES;
		else if( ch == '/' ) ch = 63;
		else flignore = YES;

		if( ! flignore )
		{
			short ctcharsinbuf = 3;
			BOOL flbreak = NO;

			if( flendtext )
			{
				if( ! ixinbuf ) break;
				if( ( ixinbuf == 1 ) || ( ixinbuf == 2 ) ) ctcharsinbuf = 1;
				else ctcharsinbuf = 2;
				ixinbuf = 3;
				flbreak = YES;
			}

			inbuf [ixinbuf++] = ch;

			if( ixinbuf == 4 )
			{
				ixinbuf = 0;
				outbuf [0] = ( inbuf[0] << 2 ) | ( ( inbuf[1] & 0x30) >> 4 );
				outbuf [1] = ( ( inbuf[1] & 0x0F ) << 4 ) | ( ( inbuf[2] & 0x3C ) >> 2 );
				outbuf [2] = ( ( inbuf[2] & 0x03 ) << 6 ) | ( inbuf[3] & 0x3F );

				for( i = 0; i < ctcharsinbuf; i++ )
					[result appendBytes:&outbuf[i] length:1];
			}

			if( flbreak )  break;
		}
	}

	return [NSData dataWithData:result];
}

@end

@interface SCRAM ()
@property (nonatomic) BOOL awaitingChallenge;
@property (nonatomic, strong) NSString *username;
@property (nonatomic, strong) NSString *password;
@property (nonatomic, strong) NSString *clientNonce;
@property (nonatomic, strong) NSString *combinedNonce;
@property (nonatomic, strong) NSString *salt;
@property (nonatomic, strong) NSNumber *count;
@property (nonatomic, strong) NSString *serverMessage1;
@property (nonatomic, strong) NSString *clientFirstMessageBare;
@property (nonatomic, strong) NSData *serverSignatureData;
@property (nonatomic, strong) NSData *clientProofData;
@property (nonatomic) CCHmacAlgorithm hashAlgorithm;

+ (NSDictionary *) dictionaryFromChallenge:(NSString*) challenge;
@end

@implementation SCRAM

- (id)initWithUsername:(NSString*)user password:(NSString *)password nonce:(NSString*)nonce {
	if ((self = [super init])) {
		self.authenticated = FALSE;
		self.username = user;
		self.password = password;
		self.clientNonce = nonce;
		self.hashAlgorithm = kCCHmacAlgSHA256;
		self.awaitingChallenge = YES;
	}
	return self;
}

- (id)initWithUsername:(NSString*)user password:(NSString *)password {
	if ((self = [super init])) {
		self.authenticated = FALSE;
		self.username = user;
		self.password = password;
		self.clientNonce = [[NSUUID UUID] UUIDString];
		self.hashAlgorithm = kCCHmacAlgSHA256;
		self.awaitingChallenge = YES;
	}
	return self;
}

- (NSString* _Nullable) handleAuth1:(NSString*) authResponse
{
	NSDictionary *auth = [SCRAM dictionaryFromChallenge:authResponse];

	NSNumberFormatter *numberFormatter = [[NSNumberFormatter alloc] init];
	[numberFormatter setNumberStyle:NSNumberFormatterDecimalStyle];

	self.serverMessage1 = authResponse;
	self.combinedNonce = [auth objectForKey:@"r"];
	self.salt = [auth objectForKey:@"s"];
	self.count = [numberFormatter numberFromString:[auth objectForKey:@"i"]];

	//We have all the necessary information to calculate client proof and server signature
	if ([self calculateProofs]) {
		self.awaitingChallenge = NO;
		return [self clientFinalMessage];
	}
	else {
		return nil;
	}
}

- (void) handleAuth2:(NSString*) authResponse {
	NSDictionary *auth = [SCRAM dictionaryFromChallenge:authResponse];
	NSString *receivedServerSignature = [auth objectForKey:@"v"];

	NSData* decoded = [[receivedServerSignature dataUsingEncoding:NSUTF8StringEncoding] base64Decoded];
	if([self.serverSignatureData isEqualToData:decoded]){
		self.authenticated = YES;
	}
	else {
		self.authenticated = NO;
	}
}

- (NSString* _Nullable) receive:(NSString*) auth {
	if (self.awaitingChallenge) {
		return [self handleAuth1:auth];
	}
	else {
		[self handleAuth2:auth];
		return nil;
	}
}

- (NSString*) clientFirstMessage {
	self.clientFirstMessageBare = [NSString stringWithFormat:@"n=%@,r=%@", self.username, self.clientNonce];

	return [NSString stringWithFormat:@"n,,%@",self.clientFirstMessageBare];
}

- (NSString*) clientFinalMessage {
	NSString *clientProofString = [self.clientProofData base64Encoded];
	return [NSString stringWithFormat:@"c=biws,r=%@,p=%@", self.combinedNonce, clientProofString];
}

- (BOOL) calculateProofs {
	// Check to see that we have a password, salt and iteration count above 4096 (from RFC5802)
	if(!self.salt.length) {
		return NO;
	}

	if(self.password.length > 0 && self.count.integerValue < 4096) {
		return NO;
	}

	NSData *passwordData = [self.password dataUsingEncoding:NSUTF8StringEncoding];
	NSData *saltData = [[self.salt dataUsingEncoding:NSUTF8StringEncoding] base64Decoded];

	NSData *saltedPasswordData = [passwordData hashPasswordWithAlgorithm:self.hashAlgorithm salt:saltData iterations:[self.count unsignedIntValue]];

	NSData *clientKeyData = [[@"Client Key" dataUsingEncoding:NSUTF8StringEncoding] hashWithAlgorithm:self.hashAlgorithm key:saltedPasswordData];
	NSData *serverKeyData = [[@"Server Key" dataUsingEncoding:NSUTF8StringEncoding] hashWithAlgorithm:self.hashAlgorithm key:saltedPasswordData];
	NSData *storedKeyData = [clientKeyData sha256Digest];

	NSData *authMessageData = [[NSString stringWithFormat:@"%@,%@,c=biws,r=%@",self.clientFirstMessageBare,self.serverMessage1,self.combinedNonce] dataUsingEncoding:NSUTF8StringEncoding];

	NSData *clientSignatureData = [authMessageData hashWithAlgorithm:self.hashAlgorithm key:storedKeyData];
	self.serverSignatureData = [authMessageData hashWithAlgorithm:self.hashAlgorithm key:serverKeyData];
	self.clientProofData = [clientKeyData xorWithData:clientSignatureData];

	//check to see that we calculated some client proof and server signature
	if (self.clientProofData && self.serverSignatureData) {
		return YES;
	}
	else {
		return NO;
	}
}

+ (NSDictionary*) dictionaryFromChallenge:(NSString*) challenge {
	// The value of the challenge stanza is base 64 encoded.
	// Once "decoded", it's just a string of key=value pairs separated by commas.
	NSArray *components = [challenge componentsSeparatedByString:@","];
	NSMutableDictionary *auth = [NSMutableDictionary dictionaryWithCapacity:5];

	for (NSString *component in components)
	{
		NSRange separator = [component rangeOfString:@"="];
		if (separator.location != NSNotFound)
		{
			NSMutableString *key = [[component substringToIndex:separator.location] mutableCopy];
			NSMutableString *value = [[component substringFromIndex:separator.location+1] mutableCopy];

			if(key) CFStringTrimWhitespace((__bridge CFMutableStringRef)key);
			if(value) CFStringTrimWhitespace((__bridge CFMutableStringRef)value);

			if ([value hasPrefix:@"\""] && [value hasSuffix:@"\""] && [value length] > 2)
			{
				// Strip quotes from value
				[value deleteCharactersInRange:NSMakeRange(0, 1)];
				[value deleteCharactersInRange:NSMakeRange([value length]-1, 1)];
			}

			if(key && value)
			{
				[auth setObject:value forKey:key];
			}
		}
	}

	return auth;
}

@end