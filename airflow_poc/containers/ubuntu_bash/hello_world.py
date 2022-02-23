import os

print("hello world!")
print(f"currently in: {os.getcwd()}")

print(os.listdir("/mnt/random"))

os.chdir('/mnt/random')

# f = open("demofile2.txt", "a")
# f.write("Now the file has more content!")
# f.close()

print(os.listdir("/mnt/random"))

print(f"currently in: {os.getcwd()}")