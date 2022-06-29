from bs4 import BeautifulSoup
import re
import requests
import time
import pandas as pd

# keep track of ratio of failed requests for code optimization (mainly for sleep time)
requests_made = 0
requests_failed = 0


# helper function (get request 'wrapper') to deal with SEC request limit and random errors
def SEC_get_request(url):
    global requests_made
    global requests_failed

    # repeat failed requests for a maximum of 20 times
    for i in range(20):
        time.sleep(0.01)  # default sleep with each request - value found through testing
        response = requests.get(url)
        requests_made += 1
        if response.status_code == 200:
            return response
        else:
            requests_failed += 1
            time.sleep(0.11)  # sleep some time just in case
    print(response.text+"\n\nPROGRAM STOPPED\n\nMaximum number of request tries reached for:\n"+url+"\nLast response status code:\n"+str(response.status_code)+"\nScroll up for last response text\n")
    print("requests made: {}".format(requests_made))
    print("requests failed: {}".format(requests_failed))
    exit(2)


# first task
# accepts a single day or a list of days as an input
# returns list of HTML version of all filings for that day/those days
# if form is set then only looks for filings of that form type
def get_SEC_filings_by_date(days, form=None):

    # check for valid days input argument
    wrong_format = False
    if type(days) == str and len(days) == 8:
        days = [days]
    elif type(days) == list:
        for day in days:
            if type(day) != str or len(day) != 8:
                wrong_format = True
                break
    else:
        wrong_format = True
    if wrong_format:
        print("Incorrect date format, must be a string of format YYYYMMDD, or a list of such strings")
        exit(1)

    url_main = 'https://www.sec.gov/Archives/edgar/'

    downloads = []
    for day in days:
        print("\n"+day+"\n")

        # get index file for given day
        year = day[0:4]
        month = day[4:6]
        quarter = str((int(month)-1)//3+1)
        url_index = url_main+'daily-index/'+year+'/QTR'+quarter+'/form.'+day+'.idx'
        index_response = SEC_get_request(url_index)

        # remove header to get filing list
        filing_list = re.split(r'File Name\s*[-]+\s+', index_response.text)[1]

        # select all appropriate forms from that day
        if form:                                 # double \s+ because some form names contain (single) spaces
            form_filing_list = re.findall(form+r'\s+\s+.+\.txt', filing_list)
            if not form_filing_list:
                print("No form '"+form+"' submitted on this day")
        else:
            form_filing_list = re.findall(r'\s+.+\.txt', filing_list)

        for form_filing in form_filing_list:
            # get the cik and identification number to access the filing index page
            filing_sub_url = re.search(r'.*edgar/(\S+)\.txt$', form_filing).group(1)
            filing_index_response = SEC_get_request(url_main+filing_sub_url+'-index.htm')
            filing_soup = BeautifulSoup(filing_index_response.text, 'html.parser')

            # obtain the form name from the filing index page
            form = filing_soup.find('div', id='formName').strong.string
            form = re.split(r'Form ', form)[1]

            # get all the rows that have the form name in the "type" column of the main table
            possible_forms = filing_soup.find_all('td', scope='row', text=form)
            print("{:12} {:35}  ".format(form, filing_sub_url), end="")

            # among them find the html version of the form (if it exists)
            html_found = False
            for p_form in possible_forms:
                try:
                    href = p_form.find_previous_sibling().a['href']
                except AttributeError:
                    href = None
                except TypeError:
                    href = None
                if href:
                    if re.search(r'\.htm$|\.html$', href):
                        form_url = re.search(r'edgar/(.*)$', href).group(1)
                        html_found = True
                        print(form_url)
                        form_response = SEC_get_request(url_main+form_url)
                        downloads.append(form_response.text)
                        break
            if not html_found:
                print("No HTML file available")

    print("")
    return downloads


# second task
# accepts a single day as an input
# returns a dataframe with the items of the 10-Q form among all the filings on that day
# if unable to obtain an item will state "NA" in the dataframe
# -------------------------------------------------------------------------------------
# works by obtaining reference to section location of an item form the table of contents
# documents may not have such a reference, in which case the function fails to obtain
# the content of the items
def get_data_from_10_Q_filings(day):
    filing_list = get_SEC_filings_by_date(day, "10-Q")

    # for finding the items in the form file and saving found data
    item_list = [
        ["part 1 item 1", "item 1", []],
        ["part 1 item 2", "item 2", []],
        ["part 1 item 3", "item 3", []],
        ["part 1 item 4", "item 4", []],
        ["part 2 item 1", "item 1", []],
        ["part 2 item 1A", "item 1A", []],
        ["part 2 item 2", "item 2", []],
        ["part 2 item 3", "item 3", []],
        ["part 2 item 4", "item 4", []],
        ["part 2 item 5", "item 5", []],
        ["part 2 item 6", "item 6", []]
        ]

    for filing in filing_list:
        soup = BeautifulSoup(filing, "html.parser")

        # find table of contents to get references to the location of the items in the document
        tables = soup.find_all('table')
        toc = None
        for table in tables:
            for string in table.stripped_strings:
                if re.search(r'financial information|other information', string, re.IGNORECASE):
                    toc = table
                    break
            if toc:
                break

        # only proceed if toc has been found
        if toc:

            # next up:
            # go through item list, locate item in soup
            # split off the section before the current item tag,
            # if i > 0 then that corresponds to the content
            # belonging to the previous item, so save it there
            # continue to work with the remaining part (after the current item tag)

            for i in range(len(item_list)):

                # get reference to item from toc
                cur_item = toc.find('a', text=re.compile(item_list[i][1], re.IGNORECASE))
                if not cur_item:
                    item_list[i-1][2].append('NA')
                else:
                    cur_href = cur_item['href'][1:]
                    cur_item.decompose()

                    # find the item section in the document using the found reference
                    item_tag = soup.find('a', attrs={'name': cur_href})
                    if not item_tag:
                        item_tag = soup.find('a', attrs={'id': cur_href})
                    if not item_tag:
                        item_list[i-1][2].append('NA')
                    else:
                        item_tag = re.match(r'(.*'+cur_href+'")', str(item_tag)).group(1)
                        re_item = re.compile(str(item_tag), re.IGNORECASE)
                        filing_parts = re_item.split(filing)

                        if i > 0:
                            item_soup = BeautifulSoup(filing_parts[0], "html.parser")
                            # clear tables and images before saving
                            for table in item_soup("table"):
                                table.decompose()
                            for img in item_soup("img"):
                                img.decompose()
                            # save content before the split to the correct item
                            item_list[i-1][2].append(str(item_soup))
                        # if last item is reached then save the remainder of the document to that item
                        if i == len(item_list)-1:
                            item_soup = BeautifulSoup(filing_parts[1], "html.parser")
                            # clear tables and images before saving
                            for table in item_soup("table"):
                                table.decompose()
                            for img in item_soup("img"):
                                img.decompose()
                            item_list[i][2].append(str(item_soup))
                        # continue to work only with remaining part
                        filing = filing_parts[1]

    df = pd.DataFrame({k:v for (k, dummy, v) in item_list})
    pd.set_option('display.max_columns', None)  # to view all columns
    print(df.head(10))
    return df


# test the functions
def main():
    runtime = time.time()

    # first task function works well for all inputs
    #get_SEC_filings_by_date("20210308", "25-NSE")
    #get_SEC_filings_by_date(["20200608", "20200609", "20200610"], "1-A")
    #get_SEC_filings_by_date("20210308", "10-Q")

    # second task function only works for specific days
    get_data_from_10_Q_filings("20210308")  # works well
    #get_data_from_10_Q_filings("20210310")  # can't find most items
    #get_data_from_10_Q_filings("20210309")  # doesn't work for most other dates; ran out of time to fix

    # some additional info
    runtime = time.time()-runtime
    print("\nruntime: {:.2f}s".format(runtime))
    print("requests made: {}".format(requests_made))
    print("requests failed: {}".format(requests_failed))


if __name__ == "__main__":
    main()
