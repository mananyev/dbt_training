{% docs payment_status %}
	
One of the following values: 

| status         | definition                                       |
|----------------|--------------------------------------------------|
| success        | Payment was successful                           |
| fail           | Payment failed                                   |
{% enddocs %}


{% docs payment_method %}

One of the following values:

| payment_method | definition                |
|----------------|---------------------------|
| credit_card    | Credit card               |
| coupon         | Coupon code               |
| bank_transfer  | Direct bank transfer      |
| gift_card      | A code from the gift card |

{% enddocs%}