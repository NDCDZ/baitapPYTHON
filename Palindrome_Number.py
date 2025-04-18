# Example 1:

# Input: x = 121
# Output: true
# Explanation: 121 reads as 121 from left to right and from right to left.

# Example 2:

# Input: x = -121
# Output: false
# Explanation: From left to right, it reads -121. From right to left, it becomes 121-. Therefore it is not a palindrome.
# Example 3:

# Input: x = 10
# Output: false
# Explanation: Reads 01 from right to left. Therefore it is not a palindrome.
 

# Constraints:

# -231 <= x <= 231 - 1

def isPalindrome(x: int) -> bool:
    if x < 0:
        return False
    # kiểm tra nếu x < 0 thì là False (số âm không phải là palindrome)

    soGoc = x 
    soDaoNguoc = 0
    
    
    while x > 0:
        so = x % 10
        soDaoNguoc = soDaoNguoc * 10 + so
        x //= 10
#     Ban đầu, doDaoNguoc = 0 .
# Mỗi lần lặp, soDaoNguoc được nhân với 10 (dịch trái các chữ số hiện có) và cộng thêm các chữ số mới ( so ).
# Ví dụ, với x = 123 :
# Lần 1: so = 3 , soDaoNguc = 0 * 10 + 3 = 3 , x = 12 .
# Lần 2: so = 2 , soDaoNguc = 3 * 10 + 2 = 32 , x = 1 .
# Lần 3: so = 1 , soDaoNguc = 32 * 10 + 1 = 321 , x = 0 .
# Kết quả là soDaoNguoc = 321 , tức là số đảo ngược của 123 .

    return soGoc == soDaoNguoc

if __name__ == "__main__":
    print("nhập 1 số nguyên để kiểm tra palindrome (Ctrl + C để out) ")
    while True:
        try:
            x = int(input("Nhập 1 số nguyên: "))
            result = isPalindrome(x)
            print(result)
        except ValueError:
            print("Vui lòng nhập số hợp lệ đi làm ơn đấy")
        except (KeyboardInterrupt):
            print("\nĐã out")
            break
    
    