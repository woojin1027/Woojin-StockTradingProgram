import newspaper


print(4//3)
# 몫 나타내기

print(bin(12345))
# 이진수로 변화

print(oct(13452))
print(hex(12345))

print("Nice to meet you")
print("Nice to " + "meet you")
print("Nice to m" + "e"*2 + "t you")
print('Nice to meet you')

num = "881120-1068234"
a = num[0:]
print(a)
print("홍길동 주민번호 : %s" %num)
print("홍길동 생년월일 : 19 %s 년" %num[0:1] +  "%s 월" %num[2:3] + " %s 일" %num[4:5])