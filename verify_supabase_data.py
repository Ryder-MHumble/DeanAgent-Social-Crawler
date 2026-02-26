"""Verify data counts in Supabase after the crawl."""
import asyncio
from database.supabase_client import get_supabase

def main():
    sb = get_supabase()
    
    # Count contents by platform
    print("=" * 60)
    print("SUPABASE DATA VERIFICATION")
    print("=" * 60)
    
    # Contents table
    print("\n📄 CONTENTS TABLE:")
    contents = sb.table("contents").select("platform, content_id", count="exact").execute()
    total_contents = len(contents.data)
    print(f"  Total: {total_contents}")
    
    for platform in ["xhs", "dy", "bili"]:
        result = sb.table("contents").select("content_id", count="exact").eq("platform", platform).execute()
        print(f"  {platform}: {len(result.data)}")
    
    # Comments table
    print("\n💬 COMMENTS TABLE:")
    comments = sb.table("comments").select("platform, comment_id", count="exact").execute()
    total_comments = len(comments.data)
    print(f"  Total: {total_comments}")
    
    for platform in ["xhs", "dy", "bili"]:
        result = sb.table("comments").select("comment_id", count="exact").eq("platform", platform).execute()
        count = len(result.data)
        if count > 0:
            print(f"  {platform}: {count}")
    
    # Creators table
    print("\n👤 CREATORS TABLE:")
    creators = sb.table("creators").select("platform, user_id", count="exact").execute()
    total_creators = len(creators.data)
    print(f"  Total: {total_creators}")
    
    for platform in ["xhs", "dy", "bili"]:
        result = sb.table("creators").select("user_id", count="exact").eq("platform", platform).execute()
        count = len(result.data)
        if count > 0:
            print(f"  {platform}: {count}")
    
    # Show a sample of comments to confirm the fix worked
    print("\n📝 SAMPLE COMMENTS (latest 5):")
    sample = sb.table("comments").select("platform, comment_id, content, nickname").order("last_modify_ts", desc=True).limit(5).execute()
    for c in sample.data:
        content_preview = (c.get("content", "") or "")[:60]
        print(f"  [{c['platform']}] {c.get('nickname','?')}: {content_preview}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()
