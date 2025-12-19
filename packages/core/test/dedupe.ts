
import { strict as assert } from 'assert';

console.log("Running Deduplication Logic Verification...");

// --- Mock Types & Updater Logic (Ported from HollowContext) ---

interface Message {
    id: string;
    content: string;
    status: 'sending' | 'sent';
    user: { id: string };
    timestamp: string;
    external?: { nonce?: string; provider?: string };
}

interface Event {
    id: string; // CGP Event ID
    body: {
        content: string;
        external?: { nonce?: string };
    };
    author: string; // User ID
    createdAt: number;
}

const currentUser = { id: "my-user-id" };

// --- Updated Updater Logic with Truncated Nonce Support ---

function updater(messages: Message[], newMessage: Message): Message[] {
    const msgs = [...messages];

    // 1. Check for Exact ID Match
    const idMatchIndex = msgs.findIndex(m => m.id === newMessage.id);
    if (idMatchIndex > -1) {
        console.log(`[Updater] ID Match: ${msgs[idMatchIndex].id} -> Update`);
        msgs[idMatchIndex] = { ...msgs[idMatchIndex], ...newMessage };
        return msgs;
    }

    // 2. Check for Nonce Match (Golden Match) - PREFIX MATCH
    // message.external.nonce might be truncated (25 chars).
    // m.id (Local Hash) is full length (64 chars).
    // We check if m.id starts with nonce OR if nonce === m.id (for tempIds if we passed tempId as nonce?)
    // Note: If we use backend.sendMessage, we pass REAL ID as nonce.
    // If store has TEMP ID, this match FAILS unless we update store first.
    const nonceMatchIndex = msgs.findIndex(m => {
        if (!newMessage.external?.nonce) return false;
        // Check if Local ID starts with Nonce (Truncated Match)
        if (m.id.startsWith(newMessage.external.nonce)) return true;
        // Check exact match (just in case)
        if (m.id === newMessage.external.nonce) return true;
        return false;
    });

    if (nonceMatchIndex > -1) {
        console.log(`[Updater] Nonce Match: ${msgs[nonceMatchIndex].id} -> Replace with ${newMessage.id}`);
        msgs[nonceMatchIndex] = newMessage;
        return msgs;
    }

    // 3. Check for Optimistic (Sending) Match - STRONG MATCH
    const optimisticIndex = msgs.findIndex(m =>
        m.status === 'sending' &&
        m.user.id === newMessage.user.id &&
        (
            m.content === newMessage.content ||
            newMessage.content?.includes(m.content?.trim() || '') ||
            (newMessage.content && m.content && newMessage.content.startsWith(m.content))
        )
    );
    if (optimisticIndex > -1) {
        console.log(`[Updater] Optimistic Match: ${msgs[optimisticIndex].id} -> Replace with ${newMessage.id}`);
        msgs[optimisticIndex] = newMessage;
        return msgs;
    }

    // NO TIMERS allowed. Pure identity matching.

    console.log(`[Updater] Append New: ${newMessage.id}`);
    return [...msgs, newMessage];
}

// --- Verification Tests ---

function testOptimisticWithNonce() {
    console.log("\n--- Test: Optimistic Client with Nonce ---");
    let messages: Message[] = [];

    // 1. User sends message (Optimistic)
    const pendingMsg: Message = {
        id: "nonce-123",
        content: "Hello World",
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];
    assert.equal(messages.length, 1);
    assert.equal(messages[0].status, "sending");

    // 2. Server Event Arrives (With Nonce)
    const serverMsg: Message = {
        id: "server-555",
        content: "Hello World",
        status: "sent", // Events are always 'sent'
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: { nonce: "nonce-123" }
    };

    messages = updater(messages, serverMsg);

    // Expectation: Replaced "nonce-123" with "server-555" (Length 1)
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-555");
    assert.equal(messages[0].status, "sent");
    console.log("PASS");
}

function testPassiveClient() {
    console.log("\n--- Test: Passive Client (Same User) ---");
    let messages: Message[] = [];

    // 1. Server Event Arrives (With Nonce, but no local pending)
    const serverMsg: Message = {
        id: "server-555",
        content: "Hello World",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: { nonce: "nonce-123" }
    };

    messages = updater(messages, serverMsg);

    // Expectation: Appended "server-555" (Length 1)
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-555");
    console.log("PASS");
}

function testOptimisticWithoutNonce() {
    console.log("\n--- Test: Optimistic Client WITHOUT Nonce (Legacy/Fallback) ---");
    let messages: Message[] = [];

    // 1. User sends message
    const pendingMsg: Message = {
        id: "local-abc",
        content: "Legacy Message",
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];

    // 2. Server Event Arrives (No Nonce)
    const serverMsg: Message = {
        id: "server-999",
        content: "Legacy Message",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString()
        // No external.nonce
    };

    messages = updater(messages, serverMsg);

    // Expectation: Matched by Content (Optimistic Match)
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-999");
    console.log("PASS");
}

function testTruncatedNonceMatch() {
    console.log("\n--- Test: Truncated Nonce (Real ID in Store) ---");
    let messages: Message[] = [];

    // 1. Local message matches Real ID (Hash)
    const realId = "02d41394b58de80ad711356994bbe5a86e297d7cda119683ab03d9a20b14dc2f8e";
    const pendingMsg: Message = {
        id: realId,
        content: "Hello World",
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];

    // 2. Server Event (Truncated Nonce)
    const serverMsg: Message = {
        id: "server-id-1",
        content: "Hello World",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: { nonce: realId.substring(0, 25) }
    };

    messages = updater(messages, serverMsg);

    // Expectation: Replace via Nonce Match
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-id-1");
    console.log("PASS");
}

function testTempIdAndTruncatedNonce() {
    console.log("\n--- Test: Temp ID in Store + Truncated Nonce Mismatch (The Race) ---");
    let messages: Message[] = [];

    // 1. Local message has TEMP ID (UUID)
    const tempId = "uuid-1234-5678";
    const realId = "02d41394b58de80ad711356994bbe5a86e297d7cda119683ab03d9a20b14dc2f8e";

    const pendingMsg: Message = {
        id: tempId, // Store has Temp ID
        content: "Race Condition",
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];

    // 2. Server Event arrives with Nonce = RealID (Truncated)
    // Because sendMessage call used the RealID as nonce.
    const serverMsg: Message = {
        id: "server-id-2",
        content: "Race Condition",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: { nonce: realId.substring(0, 25) }
    };

    // UPDATE:
    // Nonce Match: tempId.startsWith(truncatedRealId) -> FALSE.
    // Optimistic Match: Content match? "Race Condition" === "Race Condition". TRUE.

    messages = updater(messages, serverMsg);

    // Expectation: Replace via Optimistic Content Match
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-id-2");
    console.log("PASS");
}

function testPluginContentMutation() {
    console.log("\n--- Test: Plugin Content Mutation (Embeds) ---");
    let messages: Message[] = [];

    // 1. Local Temp ID
    const pendingMsg: Message = {
        id: "temp-uuid",
        content: "Check this image", // Trimmed? Matches?
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];

    // 2. Server msg has URL appended
    const serverMsg: Message = {
        id: "server-id-3",
        content: "Check this image \n https://example.com/image.png",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: { nonce: "unknown-nonce" } // Nonce mismatch simulation
    };

    // Matcher: newMessage.content.includes(m.content)
    // "Check this image \n...".includes("Check this image") -> TRUE.

    messages = updater(messages, serverMsg);

    assert.equal(messages.length, 1);
    console.log("PASS");
}

function testIdentityMismatch() {
    console.log("\n--- Test: Identity Mismatch (Should NOT Merge) ---");
    let messages: Message[] = [];
    const pendingMsg: Message = {
        id: "temp-uuid",
        content: "Hello",
        status: "sending",
        user: currentUser, // user-id
        timestamp: new Date().toISOString()
    };
    messages = [...messages, pendingMsg];

    const serverMsg: Message = {
        id: "server-id-4",
        content: "Hello",
        status: "sent",
        user: { id: "discord-user-id" }, // Different ID (e.g. is_me check failed)
        timestamp: new Date().toISOString(),
        external: { nonce: "something" }
    };

    messages = updater(messages, serverMsg);

    // Expectation: No Merge. Append.
    assert.equal(messages.length, 2);
    console.log("PASS");
}

function testDuplicateEcho() {
    console.log("\n--- Test: Duplicate Echo (Should APPEND if no Nonce/Optimistic match) ---");
    // Since we removed Time-Based dedupe, a random echo with different ID and no context MUST append.
    // This confirms "No Magic" logic.
    let messages: Message[] = [];
    const existingMsg: Message = {
        id: "server-555",
        content: "Hello World",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString()
    };
    messages = [...messages, existingMsg];

    const echoMsg: Message = {
        id: "server-DIFFERENT-ID",
        content: "Hello World",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString()
    };

    messages = updater(messages, echoMsg);

    // Expectation: APPEND (Length 2). 
    assert.equal(messages.length, 2);
    console.log("PASS");
}

function testStrongOptimistic() {
    console.log("\n--- Test: Strong Optimistic Match (Plugin mutated content) ---");
    let messages: Message[] = [];
    // Optimistic
    messages = [{
        id: "local-1",
        content: "Check image",
        status: "sending",
        user: currentUser,
        timestamp: new Date().toISOString()
    }];

    // Server says: "Check image <url>"
    const serverMsg: Message = {
        id: "server-1",
        content: "Check image https://foo.com",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString()
        // No nonce provided here, testing content match fallback
    };

    messages = updater(messages, serverMsg);

    // Expectation: REPLACE (Length 1)
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "server-1");
    // Content should be server content
    assert.ok(messages[0].content?.includes("https"));
    console.log("PASS");
}

try {
    testOptimisticWithNonce();
    testPassiveClient();
    testOptimisticWithoutNonce();
    testTruncatedNonceMatch();
    testTempIdAndTruncatedNonce();
    testPluginContentMutation();
    testIdentityMismatch();
    testDuplicateEcho();
    testStrongOptimistic();
    testStressVaryingSameness();
    testGenericProvider();
    console.log("\nAll Tests Passed!");
} catch (e) {
    console.error("\nTEST FAILED:", e);
    process.exit(1);
}

function testGenericProvider() {
    console.log("\n--- Test: Generic Provider (Matrix/Native) ---");
    let messages: Message[] = [];

    // 1. User sends message via Generic Provider
    const pendingMsg: Message = { id: "gen-local-1", content: "Generic Hello", status: "sending", user: currentUser, timestamp: new Date().toISOString() };
    messages = [...messages, pendingMsg];

    // 2. Server Event (Generic Provider w/ Nonce)
    const serverMsg: Message = {
        id: "gen-server-1",
        content: "Generic Hello",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString(),
        external: {
            nonce: "gen-local-1", // Full ID as nonce
            provider: "matrix"
        }
    };

    messages = updater(messages, serverMsg);

    // Expectation: Replace via Nonce Match (Exact or StartsWith works for equality)
    assert.equal(messages.length, 1);
    assert.equal(messages[0].id, "gen-server-1");
    console.log("PASS: Generic Provider Nonce Match worked.");

    // 3. User sends another (No Nonce support in provider?)
    const pendingMsg2: Message = { id: "gen-local-2", content: "Generic No Nonce", status: "sending", user: currentUser, timestamp: new Date().toISOString() };
    messages = [...messages, pendingMsg2];

    const serverMsg2: Message = {
        id: "gen-server-2",
        content: "Generic No Nonce",
        status: "sent",
        user: currentUser,
        timestamp: new Date().toISOString()
        // No nonce
    };

    messages = updater(messages, serverMsg2);

    // Expectation: Replace via Strong Optimistic Match
    assert.equal(messages.length, 2);
    assert.equal(messages[1].id, "gen-server-2");
    console.log("PASS: Generic Provider Optimistic Match worked.");
}

function testStressVaryingSameness() {
    console.log("\n--- Test: Stress Test (Varying Sameness / Spam) ---");
    let messages: Message[] = [];

    // Scenario: User spams "spam" 3 times.
    const m1: Message = { id: "local-1", content: "spam", status: "sending", user: currentUser, timestamp: new Date(100).toISOString() };
    const m2: Message = { id: "local-2", content: "spam", status: "sending", user: currentUser, timestamp: new Date(200).toISOString() };
    const m3: Message = { id: "local-3", content: "spam", status: "sending", user: currentUser, timestamp: new Date(300).toISOString() };

    messages = [...messages, m1, m2, m3];
    assert.equal(messages.length, 3);

    // Server echoes m1 (Optimistic Match 1)
    const s1: Message = { id: "server-1", content: "spam", status: "sent", user: currentUser, timestamp: new Date(100).toISOString() };
    // Note: No nonce provided to force Optimistic Match fallback check

    messages = updater(messages, s1);
    // Should match m1 (local-1) because it's first? Or finding any match?
    // findIndex finds FIRST match. m1 is first.
    assert.equal(messages.length, 3);
    assert.equal(messages[0].id, "server-1");
    assert.equal(messages[1].id, "local-2");

    // Server echoes m3 (Skipping m2 to test reordering/confusion)
    const s3: Message = { id: "server-3", content: "spam", status: "sent", user: currentUser, timestamp: new Date(300).toISOString() };

    messages = updater(messages, s3);
    // Should match m3 (local-3) because m1 is already sent (status match fails), m2 is sending.
    // DOES content match m2? Yes.
    // DOES id match m2? No.
    // timestamp? m2 is 200, s3 is 300.
    // Matcher: "status==sending && content==content".
    // It matches m2 (local-2) !! 
    // WAIT. If I send "spam" (m2) and "spam" (m3).
    // Server sends s3 ("spam").
    // If I match matching content to m2, then m2 becomes s3.
    // Then m3 is still pending.
    // Then server sends s2. It matches m3.
    // Result: 3 messages. Order preserved.
    // ID mapping might be swapped (m2->s3, m3->s2) BUT to the user, this is invisible/correct!

    assert.equal(messages.length, 3);
    // Let's see what it matched.
    const isM2Matched = messages.some(m => m.id === "server-3");
    assert.ok(isM2Matched);

    // Server echoes m2
    const s2: Message = { id: "server-2", content: "spam", status: "sent", user: currentUser, timestamp: new Date(200).toISOString() };
    messages = updater(messages, s2);

    assert.equal(messages.length, 3);
    assert.ok(messages.every(m => m.status === 'sent'));
    console.log("PASS: Handled 3 identical messages correctly (swapped IDs allowed but count correct)");

    // Scenario: Unique Interleaved
    // A, B, A
    messages = [];
    const u1 = { id: "loc-u1", content: "A", status: "sending", user: currentUser, timestamp: new Date(1000).toISOString() } as Message;
    const u2 = { id: "loc-u2", content: "B", status: "sending", user: currentUser, timestamp: new Date(2000).toISOString() } as Message;
    const u3 = { id: "loc-u3", content: "A", status: "sending", user: currentUser, timestamp: new Date(3000).toISOString() } as Message;
    messages = [u1, u2, u3];

    // Server sends B (u2)
    const s_u2 = { id: "srv-u2", content: "B", status: "sent", user: currentUser, timestamp: new Date(2000).toISOString() } as Message;
    messages = updater(messages, s_u2);
    // Should match u2 explicitly (content B)
    assert.equal(messages.find(m => m.content === "B")?.id, "srv-u2");
    assert.equal(messages.filter(m => m.content === "A" && m.status === "sending").length, 2);

    console.log("PASS: Interleaved Unique handled.");
}
