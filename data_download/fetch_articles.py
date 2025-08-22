import os
import time
import json
import requests

def fetch_and_save_articles(year=2023, resume_from_page=None):
    """
    Fetch all articles from OpenAlex API for a given year, with full details.
    Saves results in 'articles_{year}_new' folder, including all authors,
    real research fields, institutions, keywords, journal info, and research topic.
    """
    
    # 1. Create output folder
    folder_name = f"articles_{year}_new"
    os.makedirs(folder_name, exist_ok=True)

    output_file = os.path.join(folder_name, "all_articles_enhanced.jsonl")
    iterations_folder = os.path.join(folder_name, "iterations")
    os.makedirs(iterations_folder, exist_ok=True)

    base_url = "https://api.openalex.org/works"
    params = {
        "filter": f"publication_year:{year},institutions.country_code:us,referenced_works_count:>10",
        "per-page": 200,
        "cursor": "*"
    }

    total_downloaded = 0
    page_count = 0

    # Resume mode
    if resume_from_page:
        last_page = resume_from_page - 1
        cursor_file = os.path.join(iterations_folder, f"cursor_after_page_{last_page}.txt")
        if os.path.exists(cursor_file):
            with open(cursor_file, "r") as f:
                params["cursor"] = f.read().strip()
            for i in range(1, resume_from_page):
                prev_file = os.path.join(iterations_folder, f"articles_page_{i}.jsonl")
                if os.path.exists(prev_file):
                    with open(prev_file, "r") as f:
                        total_downloaded += sum(1 for _ in f)
            page_count = last_page
            main_file_mode = "a"
        else:
            main_file_mode = "w"
    else:
        main_file_mode = "w"

    with open(output_file, main_file_mode, encoding="utf-8") as main_file:
        has_more_pages = True

        while has_more_pages:
            page_count += 1
            print(f"Fetching page {page_count} for {year}...")

            response = requests.get(base_url, params=params)
            if response.status_code != 200:
                print(f"HTTP Error {response.status_code}")
                break

            data = response.json()
            results = data.get("results", [])

            iteration_file = os.path.join(iterations_folder, f"articles_page_{page_count}.jsonl")
            with open(iteration_file, "w", encoding="utf-8") as iter_file:
                for work in results:
                    enhanced_work = enhance_work_data(work)
                    json_line = json.dumps(enhanced_work, ensure_ascii=False) + "\n"
                    iter_file.write(json_line)
                    main_file.write(json_line)

            total_downloaded += len(results)
            print(f"Saved {len(results)} articles. Total so far: {total_downloaded}")

            cursor = data["meta"].get("next_cursor")
            if cursor:
                params["cursor"] = cursor
                with open(os.path.join(iterations_folder, f"cursor_after_page_{page_count}.txt"), "w") as cf:
                    cf.write(cursor)
            else:
                has_more_pages = False

            time.sleep(1)

    print(f"✅ Done! Downloaded {total_downloaded} articles for {year}.")


def enhance_work_data(work):
    """
    Extracts all useful details from an OpenAlex work object
    with real names for authors, fields, and institutions, plus research topic.
    """
    enhanced = {}

    # Basic info
    enhanced["id"] = work.get("id")
    enhanced["title"] = work.get("display_name")
    enhanced["publication_year"] = work.get("publication_year")
    enhanced["cited_by_count"] = work.get("cited_by_count")
    enhanced["referenced_works_count"] = work.get("referenced_works_count")
    enhanced["type"] = work.get("type")

    # Authors
    authors_list = []
    institutions_set = set()
    for auth in work.get("authorships", []):
        author_info = {
            "author_id": auth.get("author", {}).get("id"),
            "author_name": auth.get("author", {}).get("display_name"),
            "orcid": auth.get("author", {}).get("orcid"),
            "position": auth.get("author_position"),
            "institutions": [
                {
                    "institution_id": inst.get("id"),
                    "institution_name": inst.get("display_name"),
                    "country": inst.get("country"),
                    "type": inst.get("type")
                }
                for inst in auth.get("institutions", [])
            ]
        }
        for inst in auth.get("institutions", []):
            if inst.get("display_name"):
                institutions_set.add(inst.get("display_name"))
        authors_list.append(author_info)
    enhanced["authors"] = authors_list
    enhanced["authors_count"] = len(authors_list)
    enhanced["institutions_count"] = len(institutions_set)

    # Fields & topics
    fields = [topic["field"].get("display_name") for topic in work.get("topics", []) if topic.get("field")]
    domains = [topic["domain"].get("display_name") for topic in work.get("topics", []) if topic.get("domain")]
    keywords = [k.get("display_name") for k in work.get("concepts", [])] or \
               [k.get("keyword") for k in work.get("keywords", [])]

    enhanced["fields"] = sorted(set(fields))
    enhanced["domains"] = sorted(set(domains))
    enhanced["keywords"] = sorted(set(keywords))
    enhanced["is_multidisciplinary"] = len(enhanced["fields"]) > 1
    enhanced["top_keywords"] = enhanced["keywords"][:5]

    # Ratios
    ref_count = enhanced["referenced_works_count"] or 1
    enhanced["citation_per_reference_ratio"] = round((enhanced["cited_by_count"] or 0) / ref_count, 3)

    # Journal info
    if work.get("primary_location") and work["primary_location"].get("source"):
        source = work["primary_location"]["source"]
        enhanced["journal"] = {
            "name": source.get("display_name"),
            "type": source.get("type"),
            "issn": source.get("issn"),
            "is_oa": source.get("is_oa")
        }

    # Research topic (title + fields + domains + top keywords)
    topic_parts = [
        enhanced.get("title", ""),
        ", ".join(enhanced["fields"]) if enhanced["fields"] else "",
        ", ".join(enhanced["domains"]) if enhanced["domains"] else "",
        ", ".join(enhanced["top_keywords"]) if enhanced["top_keywords"] else ""
    ]
    enhanced["research_topic"] = " | ".join([p for p in topic_parts if p])

    return enhanced


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download full OpenAlex article data with research topics")
    parser.add_argument("--year", type=int, help="Publication year", required=True)
    parser.add_argument("--resume", type=int, help="Resume from page number")
    args = parser.parse_args()

    fetch_and_save_articles(year=args.year, resume_from_page=args.resume)
