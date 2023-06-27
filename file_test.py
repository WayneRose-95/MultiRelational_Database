import json 
# currency_code_list = []
# with open("currency_code_mapping.txt") as currency_code_file:
#         for line in currency_code_file:
#             country, code = line.strip().split(',')
#             currency_code_list.append(code)

# print(currency_code_list)

with open("country_data.json", encoding='utf-8') as country_code_file:
      data = json.load(country_code_file)

keys = list(data.keys())

print(keys)