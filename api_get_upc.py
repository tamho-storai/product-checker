from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/get_upc_data', methods=['POST'])
def get_upc_data():
    data = request.json  # Nhận dữ liệu từ yêu cầu HTTP

    if 'store_code' in data:
        store_code = data['store_code']

        # Lấy sale_price từ các nguồn khác nhau (Bạn cần điều chỉnh đoạn code này)
        sale_price_product = 100  # Ví dụ: Sale Price từ Product
        sale_price_ecom = 120  # Ví dụ: Sale Price từ E-commerce

        # Tạo JSON response
        response_data = {
            "upc": {
                "store_code": store_code,
                "sale_price": sale_price_product,  # Sử dụng sale_price_product hoặc sale_price_ecom tùy vào nguồn dữ liệu
                "sale_price_product": sale_price_product,
                "sale_price_ecom": sale_price_ecom
            }
        }

        return jsonify(response_data)
    else:
        return jsonify({"message": "Error: 'store_code' not found in request data"}), 400

if __name__ == '__main__':
    app.run(debug=True)
