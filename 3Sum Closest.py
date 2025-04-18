    # Given an integer array nums of length n and an integer target, find three integers in nums such that the sum is closest to target.

    # Return the sum of the three integers.

    # You may assume that each input would have exactly one solution.

    

    # Example 1:

    # Input: nums = [-1,2,1,-4], target = 1
    # Output: 2
    # Explanation: The sum that is closest to the target is 2. (-1 + 2 + 1 = 2).
    # Example 2:

    # Input: nums = [0,0,0], target = 1
    # Output: 0
    # Explanation: The sum that is closest to the target is 0. (0 + 0 + 0 = 0).
    

    # Constraints:

    # 3 <= nums.length <= 500
    # -1000 <= nums[i] <= 1000
    # -104 <= target <= 104

def threeSumClosest(nums, target):
    nums.sort()
    n = len(nums)
    closest_sum = float('inf')
    
    for i in range(n - 2):
        left = i + 1
        right = n - 1
        while left < right:
            tong = nums[i] + nums[left] + nums[right]
            if abs (tong - target) < abs(closest_sum - target):
                closest_sum = tong
            if tong < target:
                left += 1
            elif tong > target:
                right -= 1
            else:
                return tong
            
if __name__ == "__main__":
     nums = list(map(int, input("Nhập mảng số nguyên: ").split()))
     target = int(input("Nhập số nguyên mục tiêu: "))
     result = threeSumClosest(nums, target)
     print(f"Tổng gần nhất với mục tiêu là: target: {target}, result: {result}")  