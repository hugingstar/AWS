import os

file_path = "/Users/yslee/PycharmProjects/AWS/Data/AWS_Sample/2020/04/07"
file_list = os.listdir(file_path)

# for i in file_list:
#     if i.find("part") is not -1:
#         print(i)
""""""
save_path = "/Users/yslee/PycharmProjects/AWS/Results/"

for year in ['2020']:
    for month in ['04']:
        for day in ['07']:
            try:
                folder = save_path + "{}/{}/{}/".format(year, month, day)
                if not (os.path.isdir(folder)):
                    os.makedirs(os.path.join(folder))
                    print('Success to create directory : {}'.format(folder))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    print("Failed to creat directory!!!!")
                    raise
                pass

for a in ['devicetype=Air_Conditioner']:
    for date in ['date=2020-04-07']:
        try:
            folder = save_path + "{}/{}/".format(a, date)
            if not (os.path.isdir(folder)):
                os.makedirs(os.path.join(folder))
                print('Success to create directory : {}'.format(folder))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to creat directory!!!!")
                raise
            pass

for a in ['weather-batch']:
    for date in ['day', 'month1', 'realtim']:
        try:
            folder = save_path + "{}/{}/".format(a, date)
            if not (os.path.isdir(folder)):
                os.makedirs(os.path.join(folder))
                print('Success to create directory : {}'.format(folder))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to creat directory!!!!")
                raise
            pass