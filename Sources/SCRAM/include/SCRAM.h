#ifndef SCRAM_h
#define SCRAM_h

#import <Foundation/Foundation.h>

@interface SCRAM : NSObject  {
}

- (instancetype _Nonnull) initWithUsername:(NSString* _Nonnull)user password:(NSString* _Nonnull)password;
- (instancetype _Nonnull) initWithUsername:(NSString* _Nonnull)user password:(NSString* _Nonnull)password nonce:(NSString* _Nonnull)nonce;
- (NSString* _Nullable) receive:(NSString* _Nonnull) auth;

@property (nonatomic, nonnull, readonly) NSString* clientFirstMessage;
@property (nonatomic, nonnull, readonly) NSString* clientFinalMessage;
@property (nonatomic) BOOL authenticated;

@end

#endif
