# Select 组件使用指南

## 📋 概述

`dbSelect` 是一个功能完整的 jQuery Select 下拉框组件，具有以下特性：

- ✅ 支持单选和多选
- ✅ 模糊搜索功能（不区分大小写）
- ✅ 搜索框清除按钮
- ✅ 多选显示标签 + 计数（前3个标签 + 计数）
- ✅ 默认全选/取消全选按钮
- ✅ 支持最多2个自定义扩展按钮
- ✅ 支持 HTML `<select>` 标签和数组数据两种方式
- ✅ 单个选项禁用支持
- ✅ 点击外部自动关闭
- ✅ 现代化设计，支持响应式
- ✅ 丰富的回调和事件支持

## 🚀 快速开始

### 方式一：通过 HTML select 标签初始化

```html
<!-- HTML -->
<select id="mySelect">
    <option value="option1">选项1</option>
    <option value="option2">选项2</option>
    <option value="option3">选项3</option>
</select>

<script>
    // 初始化单选
    $('#mySelect').dbSelect({
        type: 'single'
    });
</script>
```

### 方式二：通过 JavaScript 数组初始化

```javascript
$('#mySelect').dbSelect({
    type: 'multiple',
    data: [
        { label: '选项1', value: 'opt1' },
        { label: '选项2', value: 'opt2' },
        { label: '选项3', value: 'opt3' }
    ]
});
```

### 方式三：完整配置示例

```javascript
$('#mySelect').dbSelect({
    type: 'multiple',                    // 多选
    data: [
        { label: '选项1', value: 'opt1' },
        { label: '选项2', value: 'opt2' }
    ],
    defaultValue: ['opt1'],              // 默认选中
    customButtons: [                     // 最多2个自定义按钮
        {
            text: '删除',
            callback: function(values) {
                console.log('删除选中项:', values);
            }
        },
        {
            text: '导出',
            callback: function(values) {
                console.log('导出选中项:', values);
            }
        }
    ],
    onSelect: function(values, type) {   // 选择变化回调
        console.log('选中的值:', values);
    },
    onCustomButton: function(index, values, text) {  // 自定义按钮点击
        console.log('按钮索引:', index, '选中值:', values, '按钮文本:', text);
    }
});
```

## 📖 完整参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `type` | String | 'single' | 选择类型：`single`（单选）或 `multiple`（多选） |
| `data` | Array | [] | 选项数据数组，格式：`[{label: '', value: '', disabled: false}]` |
| `defaultValue` | String/Array | null | 默认选中值 |
| `disabled` | Boolean | false | 是否禁用整个 select |
| `customButtons` | Array | [] | 自定义按钮数组，最多2个 |
| `onSelect` | Function | - | 选择变化时的回调 |
| `onCustomButton` | Function | - | 自定义按钮点击时的回调 |

### customButtons 按钮配置

```javascript
customButtons: [
    {
        text: '按钮文本',           // 必需，按钮显示的文本
        callback: function(values) {  // 可选，点击时的回调
            console.log(values);
        }
    }
]
```

## 💡 实际应用示例

### 示例1：基础单选

```html
<select id="selectCity"></select>

<script>
$('#selectCity').dbSelect({
    type: 'single',
    data: [
        { label: '北京', value: 'bj' },
        { label: '上海', value: 'sh' },
        { label: '深圳', value: 'sz' }
    ],
    defaultValue: 'bj',
    onSelect: function(values) {
        console.log('选中城市:', values[0]);
    }
});
</script>
```

### 示例2：多选 + 自定义按钮

```javascript
$('#selectUsers').dbSelect({
    type: 'multiple',
    data: [
        { label: '张三', value: 'u1' },
        { label: '李四', value: 'u2' },
        { label: '王五', value: 'u3' },
        { label: '赵六', value: 'u4', disabled: true }  // 禁用
    ],
    customButtons: [
        {
            text: '删除选中',
            callback: function(values) {
                if (values.length === 0) {
                    bootGrowl('请先选择', 'warning');
                    return;
                }
                showConfirm({
                    title: '确认删除',
                    message: '确定要删除这些用户吗？',
                    icon: 'warning',
                    confirmType: 'danger',
                    onConfirm: function() {
                        doPoster('/user/delete', { ids: values }, function(data) {
                            if (data.success) {
                                bootGrowl('删除成功', 'success');
                            }
                        });
                    }
                });
            }
        },
        {
            text: '导出',
            callback: function(values) {
                console.log('导出:', values);
                // 导出逻辑
            }
        }
    ],
    onSelect: function(values, type) {
        console.log('已选择:', values.length, '项');
    }
});
```

### 示例3：从 HTML 标签初始化

```html
<select id="selectDept">
    <option value="">-- 请选择 --</option>
    <option value="dept1">技术部</option>
    <option value="dept2">销售部</option>
    <option value="dept3">人资部</option>
</select>

<script>
$('#selectDept').dbSelect({
    type: 'single',
    onSelect: function(values) {
        if (values[0]) {
            console.log('选中部门:', values[0]);
        }
    }
});
</script>
```

### 示例4：动态更新值

```javascript
// 初始化
const $select = $('#mySelect').dbSelect({
    type: 'multiple',
    data: [
        { label: '项目A', value: 'a' },
        { label: '项目B', value: 'b' },
        { label: '项目C', value: 'c' }
    ]
});

// 获取组件实例
const dbSelect = $select.next('.dbcbc-select').data('dbSelect');

// 设置值
dbSelect.setValues(['a', 'b']);

// 获取值
const values = dbSelect.getValues();
console.log('当前值:', values);

// 清空
dbSelect.clear();

// 销毁
// dbSelect.destroy();
```

### 示例5：禁用选项

```javascript
$('#mySelect').dbSelect({
    type: 'multiple',
    data: [
        { label: '可用选项1', value: 'opt1' },
        { label: '已禁用选项', value: 'opt2', disabled: true },
        { label: '可用选项2', value: 'opt3' }
    ]
});
```

## 🎯 事件处理

### 监听选择变化

```javascript
const $select = $('#mySelect').dbSelect({
    type: 'multiple',
    data: [...]
});

// 通过 jQuery 事件
$select.next('.dbcbc-select').on('dbselect:change', function(e, values) {
    console.log('选中值变化:', values);
});

// 通过回调函数
$('#mySelect').dbSelect({
    onSelect: function(values, type) {
        console.log('选中值:', values, '类型:', type);
    }
});
```

### 监听自定义按钮点击

```javascript
const $select = $('#mySelect').dbSelect({
    type: 'multiple',
    customButtons: [
        {
            text: '导出',
            callback: function(values) {
                console.log('按钮点击回调:', values);
            }
        }
    ],
    onCustomButton: function(index, values, text) {
        console.log('按钮索引:', index);
        console.log('选中值:', values);
        console.log('按钮文本:', text);
    }
});

// 通过 jQuery 事件
$select.next('.dbcbc-select').on('dbselect:button', function(e, index, values) {
    console.log('自定义按钮', index, '被点击，选中值:', values);
});
```

## 🔍 搜索功能

- 支持模糊搜索，不区分大小写
- 搜索框有清除按钮
- 实时过滤选项

```javascript
// 用户可以在搜索框中输入来过滤选项
// 比如输入"北"会显示包含"北"的所有选项
```

## 📋 API 方法

获取组件实例后，可以调用以下方法：

```javascript
// 获取实例
const dbSelect = $select.next('.dbcbc-select').data('dbSelect');

// 获取当前选中值
const values = dbSelect.getValues();

// 设置选中值
dbSelect.setValues(['value1', 'value2']);

// 清空选中
dbSelect.clear();

// 销毁组件
dbSelect.destroy();
```

## 💻 单选 vs 多选

### 单选特性
- 自动关闭下拉菜单
- 无全选/取消全选按钮
- 只显示一个值

### 多选特性
- 保持下拉菜单打开
- 默认包含全选/取消全选按钮
- 支持最多2个自定义按钮
- 标签形式显示已选项

## 🎨 样式定制

Select 组件复用项目的 CSS 变量和按钮样式，您可以通过修改以下变量来定制外观：

```css
/* 在 colors.css 或 variables.css 中修改 */
--primary-color: 主色调
--bg-primary: 背景色
--text-primary: 文字色
--border-primary: 边框色
```

## 📝 注意事项

1. **选项值必须唯一** - 不同的选项应该有不同的 value
2. **默认值格式** - 单选传字符串，多选传数组
3. **最多2个自定义按钮** - 超出部分会被忽略
4. **搜索不区分大小写** - 所以大小写混合的搜索也能匹配
5. **禁用选项** - 全选/取消全选会忽略禁用的选项
6. **HTML 标签初始化** - 可以使用 `<option disabled>` 标记禁用项

## 🐛 常见问题

### Q: 如何获取已选择的值？
A: 通过回调函数或事件监听获取：
```javascript
onSelect: function(values) {
    console.log(values); // values 就是已选项的值数组
}
```

### Q: 如何设置初始选中值？
A: 使用 `defaultValue` 参数：
```javascript
$('#mySelect').dbSelect({
    defaultValue: 'value1'  // 单选
    // 或
    defaultValue: ['value1', 'value2']  // 多选
});
```

### Q: 自定义按钮如何处理选中的值？
A: 在 callback 中接收 values 参数：
```javascript
callback: function(values) {
    console.log('选中的值:', values);
}
```

### Q: 如何禁用某个选项？
A: 在数据中设置 `disabled: true`：
```javascript
data: [
    { label: '选项1', value: 'opt1' },
    { label: '禁用选项', value: 'opt2', disabled: true }
]
```

## 📞 支持

如有问题或建议，请联系开发团队。

