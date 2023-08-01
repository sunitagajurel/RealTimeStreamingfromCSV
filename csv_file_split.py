import csv
import codecs

def write_new_csv(csv_file_write, column_headers, split_number):

    writer = csv.DictWriter(csv_file_write, fieldnames=column_headers,restval='', extrasaction='ignore')
    writer.writeheader()

    return writer

def loop_through_csv(csv, split_threshold, column_headers):

    row_counter = 0
    split_number = 0
    write_csv = None

    for row in csv:

        if (row_counter == 0) or (row_counter == split_threshold):

            # close file if it already exists before writing to a new csv file
            if write_csv:
                write_csv.close()

            split_number += 1
            write_csv = codecs.open(f"posts_{split_number}.csv",'w',encoding ='utf-8')
            writer = write_new_csv(write_csv, column_headers, split_number)
            writer.writerow(row)
            row_counter = 0
        else:
            # print(row)
            writer.writerow(row)

        row_counter += 1

def main():

    # grab inputs from user
    # csv_file_path = "C:/Users/sunit/Desktop/CapstoneProject/data/nearby-all-public-posts/allposts.csv"
    split_threshold = 1000000

    with codecs.open(r"",'r',encoding ='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        column_headers = csv_reader.fieldnames
        print(column_headers)
        loop_through_csv(csv_reader, split_threshold, column_headers)

if __name__ == "__main__":
    main()