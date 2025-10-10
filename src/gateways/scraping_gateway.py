import asyncio
import json
import re
import os
import html
import random
import rollbar
from datetime import datetime
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from src.utils.constants import PATHS

DOWNLOADS_PATH = PATHS["tmp_path"]

async def try_get_text(page, selector, use_inner_html=False, retries=3):
    """Tries to get the text of an element, with retries."""
    for attempt in range(retries):
        try:
            if use_inner_html:
                text = await page.locator(selector).inner_html()
            else:
                text = await page.locator(selector).text_content()
            if text:
                cleaned_text = html.unescape(text.strip().replace('\n', ' '))
                if use_inner_html and '<br>' in cleaned_text:
                    return [name.strip() for name in cleaned_text.split('<br>')]
                return cleaned_text
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(1)
            else:
                print(f"Warning: Error trying to get text for {selector}: {str(e)}")
    return ""

async def get_image_url(page, selector):
    """Gets an image URL from an XPath selector."""
    elements = await page.query_selector_all(f'xpath={selector}')
    if elements:
        image_url = await page.evaluate('(element) => element.getAttribute("href")', elements[0])
        if image_url: return f"{image_url}&fmt=jpeg"
    return ""

async def wait_hidden_overlay(page, timeout=120000):
    """Waits for the loading overlay to disappear."""
    try:
        await page.wait_for_selector("#overlay > div", state="hidden", timeout=timeout)
    except Exception:
        pass

async def click_with_retry(page, selector, retries=3, wait_for_visible=True, timeout=120000, sleep_between=1):
    """Performs a robust click with retries."""
    last_exc = None
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', selector)
    for attempt in range(1, retries + 1):
        try:
            if wait_for_visible:
                await page.wait_for_selector(selector, state='visible', timeout=timeout)
            await wait_hidden_overlay(page, timeout=timeout // 2)
            await page.locator(selector).click(timeout=timeout)
            await wait_hidden_overlay(page, timeout=timeout // 2)
            return True
        except Exception as e:
            last_exc = e
            print(f"Warning: [click_with_retry] Attempt {attempt}/{retries} failed for {selector}: {e}")
            if attempt < retries: await asyncio.sleep(sleep_between * attempt)
    raise last_exc

async def wait_for_any(page, selectors, timeout=120000, poll_interval=0.5):
    """Waits for any selector from a list to appear."""
    deadline = asyncio.get_event_loop().time() + (timeout / 1000)
    while True:
        for s in selectors:
            sel, state = s.get('selector'), s.get('state', 'visible')
            try:
                if await page.locator(sel).count() > 0:
                    if state == 'visible':
                        await page.wait_for_selector(sel, state='visible', timeout=1000)
                    return sel
            except Exception:
                continue
        if asyncio.get_event_loop().time() > deadline: return None
        await asyncio.sleep(poll_interval)

async def extract_row_data(page, i):
    """Extracts data from a single row of the results table."""
    filing_date_header = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child(1) > th:nth-child(6)')
    request_number = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(2)')
    registry_number = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(3)')
    denomination = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(4)')
    logo_url = await get_image_url(page, f'//*[@id="MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases_hlnkCasePicture_{i-2}"]')
    
    if 'Fecha de radicación' in filing_date_header:
        filing_date = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(6)')
        expiration_date = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(7)')
        status = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(8)')
        holder = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(9)', use_inner_html=True)
        niza_class = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(10)')
        gazette_number = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(11)')
    else:
        filing_date = ""
        expiration_date = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(6)')
        status = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(7)')
        holder = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(8)', use_inner_html=True)
        niza_class = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(9)')
        gazette_number = await try_get_text(page, f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({i}) > td:nth-child(10)')
        
    case_data = {"request_number": request_number, "registry_number": registry_number, "denomination": denomination, "logo_url": str(logo_url), "filing_date": filing_date, "expiration_date": expiration_date, "status": status, "holder": holder, "niza_class": niza_class, "gazette_number": gazette_number}
    return case_data if any(case_data.values()) else None

async def extract_all_pages_data(page, logger):
    """Handles pagination and extracts data from all result pages."""
    all_cases, current_page_num = [], 1
    if not await page.locator("#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases").is_visible():
        logger.warning("Results table not found. No data to extract.")
        return []
    
    while True:
        logger.info(f"--- Extracting data from page {current_page_num}... ---")
        await wait_hidden_overlay(page)
        try:
            await page.wait_for_selector('#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child(2)', state='visible', timeout=30000)
        except Exception:
            logger.info(f"No data rows found on page {current_page_num}.")
            break
            
        for row_index in range(2, 203):
            first_cell_selector = f'#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases > tbody > tr:nth-child({row_index}) > td:nth-child(2)'
            if await page.locator(first_cell_selector).count() > 0:
                case_data = await extract_row_data(page, row_index)
                if case_data: all_cases.append(case_data)
            else:
                break
                
        next_page_selector = f"//table[@id='MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases']//span[text()='{current_page_num}']/ancestor::td/following-sibling::td[1]/a"
        if await page.locator(next_page_selector).count() > 0:
            current_page_num += 1
            logger.info(f"Navigating to the next page (now {current_page_num})...")
            target_href = await page.locator(next_page_selector).get_attribute('href')
            if target_href:
                match = re.search(r"__doPostBack\('([^']*)','([^']*)'", target_href)
                if match:
                    event_target, event_argument = match.group(1), match.group(2)
                    await page.evaluate(f"__doPostBack('{event_target}', '{event_argument}')")
                    await page.wait_for_load_state('networkidle', timeout=90000)
                    logger.info(f"Navigation to page {current_page_num} completed.")
                else:
                    logger.warning("Could not extract PostBack event from the link. Ending pagination.")
                    break
            else:
                logger.warning("Next page link does not have an href attribute. Ending pagination.")
                break
        else:
            logger.info("No more pages found. End of extraction.")
            break
            
    return all_cases

async def scrape_by_date_range(start_date, end_date, case_state, logger, headless=True, global_retries=3):
    start_safe, end_safe, global_attempt = start_date.replace("/", "_"), end_date.replace("/", "_"), 0
    normalized_state = (case_state or 'inactive').strip().lower()
    if normalized_state not in ('active', 'inactive'):
        logger.warning(f"Unknown state '{case_state}', using 'inactive' by default.")
        normalized_state = 'inactive'
        
    state_index = '0' if normalized_state == 'active' else '1'
    output_tag = 'ACTIVE' if normalized_state == 'active' else 'INACTIVE'
    output_filename = f'{DOWNLOADS_PATH}{start_safe}_{end_safe}_{output_tag}.json'
    
    while global_attempt < global_retries:
        global_attempt += 1
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=headless, downloads_path=DOWNLOADS_PATH)
                context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36", viewport={"width": 1280, "height": 900})
                page = await context.new_page()
                page.set_default_timeout(120000)
                
                await page.goto("https://sipi.sic.gov.co/sipi/Extra/Default.aspx", wait_until='networkidle')
                await click_with_retry(page, '#MainContent_lnkTMSearch')
                await click_with_retry(page, '#MainContent_ctrlTMSearch_lnkAdvanceSearch')
                await page.wait_for_selector("#MainContent_ctrlTMSearch_txtCalCreationDateStart", state='visible')
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_lnkBtnSearch")
                await wait_hidden_overlay(page)
                
                state_selector = f"#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_rbtnlLive_{state_index}"
                logger.info(f"Selecting state: {normalized_state}")
                await page.wait_for_selector(state_selector, state='visible', timeout=20000)
                await click_with_retry(page, state_selector)
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_lnkbtnSearch > span.ui-button-text")
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_ctrlCaseStatusList_gvCaseStatuss > tbody > tr.gridview_pager.alt1 > td > div:nth-child(1) > a:nth-child(1)")
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_lnkBtnSelect > span.ui-button-text")
                
                await page.evaluate(f"document.querySelector('#MainContent_ctrlTMSearch_txtCalCreationDateStart').value = '{start_date}';")
                await page.evaluate(f"document.querySelector('#MainContent_ctrlTMSearch_txtCalCreationDateEnd').value = '{end_date}';")
                await click_with_retry(page, '#MainContent_ctrlTMSearch_lnkbtnSearch > span.ui-button-text')
                
                found = await wait_for_any(page, [{'selector': "#MainContent_ctrlTMSearch_ctrlProcList_hdrNbItems"}, {'selector': "#MainContent_ctrlTM_panelCaseData"}], timeout=30000)
                if not found:
                    if await page.locator("#MainContent_ctrlTMSearch_divHelp").is_visible():
                         logger.info(f"No results found for the range {start_date} - {end_date}.")
                         await browser.close()
                         return
                    raise RuntimeError("The results page did not load.")
                    
                if "2000" in (await try_get_text(page, "#MainContent_ctrlTMSearch_ctrlProcList_hdrNbItems") or ""):
                    logger.warning(f"SKIPPED RANGE: The range {start_date} - {end_date} exceeded the 2000 trademark limit and will not be processed.")
                    await browser.close()
                    return
                    
                try:
                    pager_selector = "#MainContent_ctrlTMSearch_ctrlProcList_gvwIPCases tr.gridview_pager"
                    if await page.locator(pager_selector).count() > 0:
                        logger.info("Configuring results view...")
                        column_dropdown = page.locator(f"{pager_selector} select.no-print")
                        if await column_dropdown.count() > 0:
                            logger.info(" -> Showing 'Filing Date' column...")
                            await column_dropdown.select_option(label="Mostrar : Fecha de radicación")
                            await page.wait_for_load_state('networkidle', timeout=60000)
                            
                        results_per_page_dropdown = page.locator(f"{pager_selector} select:not(.no-print)")
                        if await results_per_page_dropdown.count() > 0:
                            logger.info(" -> Changing to 200 results per page...")
                            await results_per_page_dropdown.select_option(value="200")
                            await page.wait_for_load_state('networkidle', timeout=60000)
                except Exception as e:
                    logger.warning(f"An error occurred while configuring the results view. Continuing. Error: {e}")
                    
                list_cases = await extract_all_pages_data(page, logger)
                with open(output_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(list_cases, json_file, ensure_ascii=False, indent=4)
                logger.info(f"SUCCESS: Saved {len(list_cases)} records ({output_tag}) for the range {start_date} - {end_date}.")
                await browser.close()
                return
                
        except Exception as e:
            logger.error(f"[scrape_by_date_range] Attempt {global_attempt}/{global_retries} failed for {start_date} - {end_date}: {e}", exc_info=True)
            rollbar.report_exc_info()
            if global_attempt >= global_retries:
                logger.critical(f"{start_date} - {end_date} -> Failed after {global_retries} attempts.")
                return
            await asyncio.sleep(2 ** global_attempt + random.random())

async def scrape_by_niza_class(niza_class, logger, headless=True, global_retries=3):
    start, end, case_state = "01/01/1900", "01/01/1900", 'active'
    output_filename = f'{DOWNLOADS_PATH}niza_{niza_class}_1900_1900_ACTIVE.json'
    global_attempt, state_index = 0, '0'
    
    while global_attempt < global_retries:
        global_attempt += 1
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=headless, downloads_path=DOWNLOADS_PATH)
                context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36", viewport={"width": 1280, "height": 900})
                page = await context.new_page()
                page.set_default_timeout(120000)

                await page.goto("https://sipi.sic.gov.co/sipi/Extra/Default.aspx", wait_until='networkidle')
                await click_with_retry(page, '#MainContent_lnkTMSearch')
                await click_with_retry(page, '#MainContent_ctrlTMSearch_lnkAdvanceSearch')
                await page.wait_for_selector("#MainContent_ctrlTMSearch_txtCalCreationDateStart", state='visible')
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_lnkBtnSearch")
                await wait_hidden_overlay(page)
                
                state_selector = f"#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_rbtnlLive_{state_index}"
                logger.info(f"Selecting state: {case_state}")
                await page.wait_for_selector(state_selector, state='visible', timeout=20000)
                await click_with_retry(page, state_selector)
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_lnkbtnSearch > span.ui-button-text")
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_ctrlCaseStatusSearch_ctrlCaseStatusList_gvCaseStatuss > tbody > tr.gridview_pager.alt1 > td > div:nth-child(1) > a:nth-child(1)")
                await click_with_retry(page, "#MainContent_ctrlTMSearch_ctrlCaseStatusSearchDialog_lnkBtnSelect > span.ui-button-text")
                
                logger.info(f"Filtering by Niza Class: {niza_class}")
                await page.fill("#MainContent_ctrlTMSearch_txtNiceClassification", str(niza_class))
                await page.evaluate(f"document.querySelector('#MainContent_ctrlTMSearch_txtCalCreationDateStart').value = '{start}';")
                await page.evaluate(f"document.querySelector('#MainContent_ctrlTMSearch_txtCalCreationDateEnd').value = '{end}';")
                await click_with_retry(page, '#MainContent_ctrlTMSearch_lnkbtnSearch > span.ui-button-text')
                
                found = await wait_for_any(page, [{'selector': "#MainContent_ctrlTMSearch_ctrlProcList_hdrNbItems"}, {'selector': "#MainContent_ctrlTM_panelCaseData"}], timeout=30000)
                if not found:
                    if await page.locator("#MainContent_ctrlTMSearch_divHelp").is_visible():
                         logger.info(f"No results found for Niza class {niza_class}.")
                         with open(output_filename, 'w', encoding='utf-8') as json_file: json.dump([], json_file)
                         await browser.close()
                         return
                    raise RuntimeError("The results page did not load.")
                    
                if "2000" in (await try_get_text(page, "#MainContent_ctrlTMSearch_ctrlProcList_hdrNbItems") or ""):
                    logger.warning(f"SKIPPED RANGE: Niza class {niza_class} exceeded the 2000 trademark limit.")
                    await browser.close()
                    return
                    
                list_cases = await extract_all_pages_data(page, logger)
                with open(output_filename, 'w', encoding='utf-8') as json_file:
                    json.dump(list_cases, json_file, ensure_ascii=False, indent=4)
                logger.info(f"SUCCESS: Saved {len(list_cases)} records for Niza class {niza_class}.")
                await browser.close()
                return
                
        except Exception as e:
            logger.error(f"[scrape_by_niza_class] Attempt {global_attempt}/{global_retries} failed for Niza {niza_class}: {e}", exc_info=True)
            rollbar.report_exc_info()
            if global_attempt >= global_retries:
                logger.critical(f"Niza {niza_class} -> Failed after {global_retries} attempts.")
                return
            await asyncio.sleep(2 ** global_attempt + random.random())

async def _extract_status_with_retries(page, logger, max_attempts=4):
    """Auxiliary function to try extracting the status from the page."""
    status_selectors = ['#MainContent_ctrlTM_lblCurrentStatus', '#MainContent_ctrlIRD_lblCurrentStatus']
    for attempt in range(1, max_attempts + 1):
        logger.debug(f"Attempt {attempt}/{max_attempts} to extract status...")
        for selector in status_selectors:
            try:
                timeout = 2000 * attempt
                await page.wait_for_selector(selector, state='visible', timeout=timeout)
                status = await page.text_content(selector)
                if status and status.strip():
                    logger.debug(f"Status found with selector '{selector}': {status.strip()}")
                    return status.strip()
            except PlaywrightTimeoutError:
                logger.debug(f"Selector '{selector}' not found on attempt {attempt}.")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error while reading selector '{selector}': {e}")
                continue
        if attempt < max_attempts:
            await asyncio.sleep(1 * attempt)
    return ""

async def scrape_request_by_number(page, request_number, logger):
    """Scrapes a single request by its number and extracts its status."""
    logger.info(f"Starting scrape for request_number: {request_number}")
    try:
        await page.goto("https://sipi.sic.gov.co/sipi/Extra/Default.aspx", wait_until='networkidle')
        await click_with_retry(page, '#MainContent_lnkTMSearch')
        
        await page.wait_for_selector('#MainContent_ctrlTMSearch_txtAppNr', state='visible', timeout=20000)
        await page.fill('#MainContent_ctrlTMSearch_txtAppNr', request_number)
        await click_with_retry(page, '#MainContent_ctrlTMSearch_lnkbtnSearch')
        await page.wait_for_load_state('networkidle', timeout=60000)
        status = await _extract_status_with_retries(page, logger)
        if status:
            return status, None
        
        result_link_selector = '#MainContent_ctrlTMSearch_gvSearchResults a'
        if await page.locator(result_link_selector).count() > 0:
            logger.info("Found a link in the results, clicking it...")
            await click_with_retry(page, result_link_selector)
            await page.wait_for_load_state('networkidle', timeout=60000)
            status = await _extract_status_with_retries(page, logger, max_attempts=5)
            if status:
                return status, None
        
        logger.warning(f"Could not determine status for {request_number} after all attempts.")
        return "", "Status not found on results page."
    except Exception as e:
        logger.error(f"Fatal error during scraping of {request_number}: {e}", exc_info=True)
        rollbar.report_exc_info()
        return "", str(e)

async def run_scraping_for_missing_requests(csv_path, logger):
    """Reads a CSV of missing records, scrapes them, and saves the results to JSON."""
    logger.info(f"Starting scraping process for missing records from '{csv_path}'")
    try:
        df = pd.read_csv(csv_path)
        if 'missing_request_number' not in df.columns:
            logger.error("The CSV must contain the column 'missing_request_number'.")
            return None
        requests_to_process = df['missing_request_number'].dropna().tolist()
    except FileNotFoundError:
        logger.error(f"The CSV file '{csv_path}' was not found.")
        return None
    
    if not requests_to_process:
        logger.info("The CSV contains no records to process.")
        return None
        
    logger.info(f"{len(requests_to_process)} records from the CSV will be processed.")
    
    all_results = []
    output_json_path = os.path.join(DOWNLOADS_PATH, f"missing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36")
            page = await context.new_page()

            for i, req_num in enumerate(requests_to_process):
                logger.info(f"Processing {i+1}/{len(requests_to_process)}: {req_num}")
                status, error = await scrape_request_by_number(page, req_num, logger)
                
                result = {"request_number": req_num, "extracted_status": status, "error": error}
                all_results.append(result)

                if (i + 1) % 50 == 0:
                    with open(output_json_path, 'w', encoding='utf-8') as f:
                        json.dump(all_results, f, indent=4, ensure_ascii=False)
                    logger.info(f"Progress saved to '{output_json_path}'")
            await browser.close()
    finally:
        if all_results:
            with open(output_json_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=4, ensure_ascii=False)
            logger.info(f"Process finished. Final results saved to '{output_json_path}'.")
        else:
            logger.warning("No results were generated to save.")

    if all_results:
        return output_json_path
    else:
        return None