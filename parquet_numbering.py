import glob
f = open("filenumber.txt", 'w')

for i in range(1, 31):
    if i < 10:
        Dic2 = 'date=2020-04-0{}'.format(i)
    elif i >= 10:
        Dic2 = 'date=2020-04-{}'.format(i)
    files = glob.glob(Dic2 + '/*.parquet')
    text = "[{}] : {}\n".format(Dic2, len(files))
    f.write(text)
    print(text)
f.close()