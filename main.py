from scraper import get_counselor_list

def run():

    counselors = get_counselor_list()

    for c in counselors:
        print(c)

if __name__ == "__main__":
    run()
