package FastSort;

// https://www.cnblogs.com/0201zcr/p/4763806.html
public class FastSort {
    /**
     * 查找出中轴（默认是最低位low）的在numbers数组排序后所在位置
     *
     * @param low  开始位置
     * @param high 结束位置
     * @return 中轴所在位置
     */
    public static int getMid(int[] arr, int low, int high) {
        int temp = arr[low];
        while (low < high) {
            while (low < high && arr[high] > temp) {
                high--;
            }
            //比中轴小的记录移到低端
            arr[low] = arr[high];
            while (low < high && arr[low] < temp) {
                low++;
            }
            //比中轴大的记录移到高端
            arr[high] = arr[low];
        }
        //中轴记录到尾
        arr[low] = temp;
        return low;
    }
}
