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
    
    # Đảo ngược số
    while x > 0:
        so = x % 10
        soDaoNguoc = soDaoNguoc * 10 + so
        x //= 10
    

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
    
    